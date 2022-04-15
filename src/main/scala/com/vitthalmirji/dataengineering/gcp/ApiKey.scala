package com.vitthalmirji.dataengineering.gcp

/**
 * [[com.vitthalmirji.dataengineering.gcp.ApiKey]]
 * Case class for GCP API Key
 *
 * @param `type`                      Account Type viz. service_account
 * @param project_id                  GCP Project ID
 * @param private_key_id              Private Key ID
 * @param private_key                 Private Key
 * @param client_email                Email of client
 * @param client_id                   Client ID
 * @param auth_uri                    URI to use for authentication
 * @param token_uri                   URI token
 * @param auth_provider_x509_cert_url Authenticating Certificate URL
 * @param client_x509_cert_url        Client Certificate URL
 */
final case class ApiKey(`type`: String, project_id: String, private_key_id: String, private_key: String,
                        client_email: String, client_id: String, auth_uri: String, token_uri: String,
                        auth_provider_x509_cert_url: String, client_x509_cert_url: String)
