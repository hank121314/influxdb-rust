//! Client which can read and write data from InfluxDBv2.
//!
//! # Arguments
//!
//!  * `url`: The URL where InfluxDB is running (ex. `http://localhost:8086`).
//!  * `bucket`: The Bucket against which queries and writes will be run.
//!
//! # Examples
//!
//! ```rust
//! use influxdb::ClientV2;
//!
//! let client = ClientV2::new("http://localhost:8086", "test");
//!
//! assert_eq!(client.database_name(), "test");
//! ```
use reqwest::{Client as ReqwestClient, StatusCode, header::{HeaderMap, HeaderValue}};

use crate::query::QueryTypes;
use crate::Error;
use crate::Query;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Debug)]
/// Internal Representation of a Client
pub struct ClientV2 {
  pub(crate) url: Arc<String>,
  pub(crate) headers: Arc<HeaderMap<HeaderValue>>,
  pub(crate) parameters: Arc<HashMap<&'static str, String>>,
}

impl ClientV2 {
    /// Instantiates a new [`ClientV2`](crate::ClientV2)
    ///
    /// # Arguments
    ///
    ///  * `url`: The URL where InfluxDB is running (ex. `http://localhost:8086`).
    ///  * `database`: The Database against which queries and writes will be run.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use influxdb::ClientV2;
    ///
    /// let _client = ClientV2::new("http://localhost:8086", "YOURAUTHTOKEN");
    /// ```
    pub fn new<S1, S2, S3>(url: S1, token: &str, org: S2, bucket: S3) -> Self
      where
        S1: Into<String>,
        S2: Into<String>,
        S3: Into<String>,
    {
      let mut headers = HeaderMap::new();
      let mut parameters = HashMap::<&str, String>::new();
      parameters.insert("org", org.into());
      parameters.insert("bucket", bucket.into());
      headers.insert("Authorization", HeaderValue::from_str(&format!("Token {}", token)).unwrap());
      ClientV2 {
        url: Arc::new(url.into()),
        headers: Arc::new(headers),
        parameters: Arc::new(parameters)
      }
    }

    /// Returns the name of the bucket the client is using
    pub fn token(&self) -> &str {
      self.get_header_by_name("Authorization")
    }

    fn get_header_by_name(&self, name: &str) -> &str {
      // safe to unwrap: we always set the organization name in `Self::new`
      if let Ok(value) = self.headers.get(name).unwrap().to_str() {
        return value;
      }

      ""
    }

    /// Returns the URL of the InfluxDB installation the client is using
    pub fn database_url(&self) -> &str {
      &self.url
    }

    /// Pings the InfluxDB Server
    ///
    /// Returns a tuple of build type and version number
    pub async fn ping(&self) -> Result<(String, String), Error> {
      let url = &format!("{}/ping", self.url);
      let client = ReqwestClient::new();
      let res = client
        .get(url)
        .send()
        .await
        .map_err(|err| Error::ProtocolError {
          error: format!("{}", err),
        })?;
      let headers = res.headers();

      let build = headers["X-Influxdb-Build"].to_str().unwrap();
      let version = headers["X-Influxdb-Version"].to_str().unwrap();

      Ok((build.to_owned(), version.to_owned()))
    }

    /// Sends a [`ReadQuery`](crate::ReadQuery) or [`WriteQuery`](crate::WriteQuery) to the InfluxDB Server.
    ///
    /// A version capable of parsing the returned string is available under the [serde_integration](crate::integrations::serde_integration)
    ///
    /// # Arguments
    ///
    ///  * `q`: Query of type [`ReadQuery`](crate::ReadQuery) or [`WriteQuery`](crate::WriteQuery)
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use influxdb::{Client, Query, Timestamp};
    /// use influxdb::InfluxDbWriteable;
    /// use std::time::{SystemTime, UNIX_EPOCH};
    ///
    /// # #[async_std::main]
    /// # async fn main() -> Result<(), influxdb::Error> {
    /// let start = SystemTime::now();
    /// let since_the_epoch = start
    ///   .duration_since(UNIX_EPOCH)
    ///   .expect("Time went backwards")
    ///   .as_millis();
    ///
    /// let client = Client::new("http://localhost:8086", "test");
    /// let query = Timestamp::Milliseconds(since_the_epoch)
    ///     .into_query("weather")
    ///     .add_field("temperature", 82);
    /// let results = client.query(&query).await?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    /// # Errors
    ///
    /// If the function can not finish the query,
    /// a [`Error`] variant will be returned.
    ///
    /// [`Error`]: enum.Error.html
    pub async fn query<'q, Q>(&self, q: &'q Q) -> Result<String, Error>
      where
        Q: Query,
        &'q Q: Into<QueryTypes<'q>>,
    {
      let client = ReqwestClient::new();
      let query = q.build().map_err(|err| Error::InvalidQueryError {
        error: err.to_string(),
      })?;

      let request_builder = match q.into() {
        QueryTypes::Read(_) => {
          let read_query = query.get();
          let headers = self.headers.as_ref().clone();
          let parameters = self.parameters.as_ref().clone();
          let url = &format!("{}/query", &self.url);

          if read_query.contains("SELECT") || read_query.contains("SHOW") {
            client.get(url).headers(headers).query(&parameters)
          } else {
            client.post(url).headers(headers).query(&parameters)
          }
        }
        QueryTypes::Write(write_query) => {
          let url = &format!("{}/api/v2/write", &self.url);
          let headers = self.headers.as_ref().clone();
          let mut parameters = self.parameters.as_ref().clone();
          parameters.insert("precision", write_query.get_precision());

          client.post(url).headers(headers).body(query.get()).query(&parameters)
        }
      }.build();

      let request = request_builder.map_err(|err| Error::UrlConstructionError {
        error: err.to_string(),
      })?;
      let res = client
        .execute(request)
        .await
        .map_err(|err| Error::ConnectionError {
          error: err.to_string(),
        })?;



      match res.status() {
        StatusCode::UNAUTHORIZED => return Err(Error::AuthorizationError),
        StatusCode::FORBIDDEN => return Err(Error::AuthenticationError),
        _ => {}
      }

      let s = res
        .text()
        .await
        .map_err(|_| Error::DeserializationError {
          error: "response could not be converted to UTF-8".to_string(),
        })?;

      // todo: improve error parsing without serde
      if s.contains("\"error\"") {
        return Err(Error::DatabaseError {
          error: format!("influxdb error: \"{}\"", s),
        });
      }

      Ok(s)
    }
}

#[cfg(test)]
mod tests {
  use super::ClientV2;

  #[test]
  fn test_fn_database() {
    let client = ClientV2::new("http://localhost:8068", "YOURAUTHTOKEN", "org", "bucket");
    assert_eq!(client.token(), "Token YOURAUTHTOKEN");
    let parameters = client.parameters;
    assert_eq!(parameters.len(), 2);
    assert_eq!(parameters.get("org").unwrap(), "org");
    assert_eq!(parameters.get("bucket").unwrap(), "bucket");
  }
}