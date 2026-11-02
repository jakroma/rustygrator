//! TLS for PostgreSQL and ScyllaDB connections (CONN-3), configured like libpq through the
//! connection string: `sslmode=disable|require|verify-ca|verify-full`, `sslrootcert=<CA file>`,
//! `sslcert=<client certificate>`, `sslkey=<client key>` (PEM files). MongoDB has its own TLS
//! options in the connection string, handled by the driver.

use std::{path::PathBuf, sync::Arc};

use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider},
    pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer, ServerName, UnixTime},
    CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};

use crate::{DMError, DMResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    /// Encrypted, the certificate is not checked
    Require,
    /// Certificate chain checked, host name not checked
    VerifyCa,
    /// Certificate chain and host name checked
    VerifyFull,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsOptions {
    pub mode: SslMode,
    pub root_cert: Option<PathBuf>,
    pub client_cert: Option<PathBuf>,
    pub client_key: Option<PathBuf>,
}

impl TlsOptions {
    /// TLS options from the query of a connection string; `None` without `sslmode` or with
    /// `sslmode=disable` / `allow` / `prefer`.
    pub fn from_query<'a>(pairs: impl Iterator<Item = (std::borrow::Cow<'a, str>, std::borrow::Cow<'a, str>)>) -> DMResult<Option<TlsOptions>> {
        let mut mode = None;
        let (mut root_cert, mut client_cert, mut client_key) = (None, None, None);
        for (k, v) in pairs {
            match k.as_ref() {
                "sslmode" => {
                    mode = match v.as_ref() {
                        "disable" | "allow" | "prefer" => None,
                        "require" => Some(SslMode::Require),
                        "verify-ca" => Some(SslMode::VerifyCa),
                        "verify-full" => Some(SslMode::VerifyFull),
                        other => return Err(DMError::InvalidConnectionString(format!("sslmode={other}"))),
                    }
                }
                "sslrootcert" => root_cert = Some(PathBuf::from(v.as_ref())),
                "sslcert" => client_cert = Some(PathBuf::from(v.as_ref())),
                "sslkey" => client_key = Some(PathBuf::from(v.as_ref())),
                _ => {}
            }
        }
        Ok(mode.map(|mode| TlsOptions { mode, root_cert, client_cert, client_key }))
    }

    /// rustls client configuration for these options.
    pub fn client_config(&self) -> DMResult<ClientConfig> {
        let err = |what: &str, e: &dyn std::fmt::Display| DMError::Error(format!("TLS {what}: {e}"));
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut roots = RootCertStore::empty();
        match &self.root_cert {
            Some(path) => {
                for cert in CertificateDer::pem_file_iter(path).map_err(|e| err(&format!("CA file {}", path.display()), &e))? {
                    roots.add(cert.map_err(|e| err("CA file", &e))?).map_err(|e| err("CA certificate", &e))?;
                }
            }
            None => {
                for cert in rustls_native_certs::load_native_certs().certs {
                    let _ = roots.add(cert);
                }
            }
        }
        let verifier: Arc<dyn ServerCertVerifier> = match self.mode {
            SslMode::Require => Arc::new(AcceptAnyCertificate(provider.clone())),
            mode => {
                let webpki = rustls::client::WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider.clone())
                    .build()
                    .map_err(|e| err("CA certificates", &e))?;
                if mode == SslMode::VerifyCa {
                    Arc::new(IgnoreHostName(webpki))
                } else {
                    webpki
                }
            }
        };
        let builder = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| err("protocol", &e))?
            .dangerous()
            .with_custom_certificate_verifier(verifier);
        match (&self.client_cert, &self.client_key) {
            (Some(cert), Some(key)) => {
                let chain = CertificateDer::pem_file_iter(cert)
                    .map_err(|e| err(&format!("client certificate {}", cert.display()), &e))?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| err("client certificate", &e))?;
                let key = PrivateKeyDer::from_pem_file(key).map_err(|e| err(&format!("client key {}", key.display()), &e))?;
                builder.with_client_auth_cert(chain, key).map_err(|e| err("client certificate", &e))
            }
            (None, None) => Ok(builder.with_no_client_auth()),
            _ => Err(DMError::InvalidConnectionString("sslcert and sslkey must be given together".into())),
        }
    }
}

/// `sslmode=require`: encrypted, but any certificate is accepted (signatures are still checked).
#[derive(Debug)]
struct AcceptAnyCertificate(Arc<CryptoProvider>);

impl ServerCertVerifier for AcceptAnyCertificate {
    fn verify_server_cert(&self, _: &CertificateDer<'_>, _: &[CertificateDer<'_>], _: &ServerName<'_>, _: &[u8], _: UnixTime) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn verify_tls13_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// `sslmode=verify-ca`: the chain must lead to a trusted CA, the host name may differ.
#[derive(Debug)]
struct IgnoreHostName(Arc<rustls::client::WebPkiServerVerifier>);

impl ServerCertVerifier for IgnoreHostName {
    fn verify_server_cert(&self, end_entity: &CertificateDer<'_>, intermediates: &[CertificateDer<'_>], server_name: &ServerName<'_>, ocsp: &[u8], now: UnixTime) -> Result<ServerCertVerified, rustls::Error> {
        match self.0.verify_server_cert(end_entity, intermediates, server_name, ocsp, now) {
            Err(rustls::Error::InvalidCertificate(CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. })) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(q: &str) -> DMResult<Option<TlsOptions>> {
        TlsOptions::from_query(url::form_urlencoded::parse(q.as_bytes()))
    }

    #[test]
    fn conn3_sslmode_parameters() {
        assert_eq!(parse("schema=x").unwrap(), None);
        assert_eq!(parse("sslmode=disable").unwrap(), None);
        let t = parse("sslmode=verify-ca&sslrootcert=ca.pem&sslcert=c.pem&sslkey=k.pem").unwrap().unwrap();
        assert_eq!(t.mode, SslMode::VerifyCa);
        assert_eq!(t.root_cert, Some("ca.pem".into()));
        assert!(parse("sslmode=sometimes").is_err());
    }

    #[test]
    fn conn3_client_configs_build() {
        // system roots
        parse("sslmode=verify-full").unwrap().unwrap().client_config().unwrap();
        parse("sslmode=require").unwrap().unwrap().client_config().unwrap();
        let missing = parse("sslmode=verify-ca&sslrootcert=does-not-exist.pem").unwrap().unwrap().client_config();
        assert!(missing.unwrap_err().to_string().contains("does-not-exist.pem"));
        assert!(parse("sslmode=require&sslcert=c.pem").unwrap().unwrap().client_config().is_err());
    }
}
