//! Self-signed TLS certificate generation for testing.
//!
//! This module is available behind the `test-tls` feature flag and provides a
//! helper to generate a CA + server certificate pair suitable for local Redis
//! TLS testing.
//!
//! # Example
//!
//! ```no_run
//! use redis_server_wrapper::tls::generate_test_certs;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let certs = generate_test_certs("/tmp/my-test/tls")?;
//!
//! // Use with a server builder:
//! // server.tls_cert_file(&certs.cert_file)
//! //       .tls_key_file(&certs.key_file)
//! //       .tls_ca_cert_file(&certs.ca_cert_file)
//! # Ok(())
//! # }
//! ```

use std::fs;
use std::path::{Path, PathBuf};

use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose};

use crate::error::{Error, Result};

/// Paths to generated test certificates.
#[derive(Debug, Clone)]
pub struct TestCerts {
    /// Path to the CA certificate file (PEM).
    pub ca_cert_file: PathBuf,
    /// Path to the server certificate file (PEM).
    pub cert_file: PathBuf,
    /// Path to the server private key file (PEM).
    pub key_file: PathBuf,
}

/// Generate a self-signed CA and server certificate for testing.
///
/// Creates three files in `dir`:
/// - `ca.crt` -- the CA certificate
/// - `server.crt` -- the server certificate signed by the CA
/// - `server.key` -- the server private key
///
/// The server certificate includes `localhost` and `127.0.0.1` as Subject
/// Alternative Names so it works for local test clusters without hostname
/// verification issues.
pub fn generate_test_certs(dir: impl AsRef<Path>) -> Result<TestCerts> {
    let dir = dir.as_ref();
    fs::create_dir_all(dir).map_err(Error::Io)?;

    // Generate CA key pair and certificate.
    let ca_key = KeyPair::generate().map_err(|e| Error::Tls(e.to_string()))?;
    let mut ca_params =
        CertificateParams::new(Vec::<String>::new()).map_err(|e| Error::Tls(e.to_string()))?;
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "Redis Test CA");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages.push(KeyUsagePurpose::KeyCertSign);
    ca_params.key_usages.push(KeyUsagePurpose::CrlSign);
    let ca_cert = ca_params
        .self_signed(&ca_key)
        .map_err(|e| Error::Tls(e.to_string()))?;

    // Generate server key pair and certificate signed by the CA.
    let server_key = KeyPair::generate().map_err(|e| Error::Tls(e.to_string()))?;
    let server_san = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let mut server_params =
        CertificateParams::new(server_san).map_err(|e| Error::Tls(e.to_string()))?;
    server_params
        .distinguished_name
        .push(DnType::CommonName, "Redis Test Server");
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .map_err(|e| Error::Tls(e.to_string()))?;

    // Write files.
    let ca_cert_file = dir.join("ca.crt");
    let cert_file = dir.join("server.crt");
    let key_file = dir.join("server.key");

    fs::write(&ca_cert_file, ca_cert.pem()).map_err(Error::Io)?;
    fs::write(&cert_file, server_cert.pem()).map_err(Error::Io)?;
    fs::write(&key_file, server_key.serialize_pem()).map_err(Error::Io)?;

    Ok(TestCerts {
        ca_cert_file,
        cert_file,
        key_file,
    })
}
