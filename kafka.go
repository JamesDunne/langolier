package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"github.com/Shopify/sarama"
	"io/ioutil"
	"langolier/env"
	"log"
	"path/filepath"
	"strings"
)

func tlsConfig(conf *sarama.Config, certBytes []byte) *sarama.Config {
	conf.Net.TLS.Enable = true
	conf.Net.TLS.Config = &tls.Config{}
	var ok bool
	conf.Net.TLS.Config.RootCAs, ok = poolFromPEMBytes(certBytes)
	if !ok {
		log.Fatalln("no certs read")
	}
	return conf
}

func gssapiKeytabConfig(conf *sarama.Config, krb5Bytes []byte, username, realm string, keytabBytes []byte) *sarama.Config {
	var err error

	krb5Path := filepath.Join(processTmpDir, "krb5.conf")
	err = ioutil.WriteFile(krb5Path, krb5Bytes, 0600)
	if err != nil {
		log.Fatalln(err)
	}

	keytabPath := filepath.Join(processTmpDir, "keytab")
	err = ioutil.WriteFile(keytabPath, keytabBytes, 0600)
	if err != nil {
		log.Fatalln(err)
	}

	conf.Net.SASL.GSSAPI.AuthType = sarama.KRB5_KEYTAB_AUTH
	conf.Net.SASL.GSSAPI.KerberosConfigPath = krb5Path
	conf.Net.SASL.GSSAPI.ServiceName = "kafka"
	conf.Net.SASL.GSSAPI.Username = username
	conf.Net.SASL.GSSAPI.Realm = realm
	conf.Net.SASL.GSSAPI.KeyTabPath = keytabPath
	return conf
}

func gssapiUserConfig(conf *sarama.Config, krb5Bytes []byte, username, realm, password string) *sarama.Config {
	var err error

	krb5Path := filepath.Join(processTmpDir, "krb5.conf")
	err = ioutil.WriteFile(krb5Path, krb5Bytes, 0600)
	if err != nil {
		log.Fatalln(err)
	}

	conf.Net.SASL.GSSAPI.AuthType = sarama.KRB5_USER_AUTH
	conf.Net.SASL.GSSAPI.KerberosConfigPath = krb5Path
	conf.Net.SASL.GSSAPI.ServiceName = "kafka"
	conf.Net.SASL.GSSAPI.Username = username
	conf.Net.SASL.GSSAPI.Realm = realm
	conf.Net.SASL.GSSAPI.Password = password
	return conf
}

func poolFromPEMFiles(paths ...string) (pool *x509.CertPool) {
	var err error
	pool = x509.NewCertPool()

	for _, path := range paths {
		var pemBytes []byte
		pemBytes, err = ioutil.ReadFile(path)
		if err != nil {
			log.Fatalln(err)
		}

		if !pool.AppendCertsFromPEM(pemBytes) {
			log.Fatalf("no certs added from PEM file %s\n", path)
		}
	}

	return
}

func poolFromPEMBytes(pemBytes []byte) (pool *x509.CertPool, ok bool) {
	pool = x509.NewCertPool()
	ok = pool.AppendCertsFromPEM(pemBytes)
	return
}

func createKafkaClient() (addrs []string, conf *sarama.Config, err error) {
	conf = sarama.NewConfig()
	conf.ClientID = "core-eventing-ds-adapter"
	conf.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	conf.Consumer.Offsets.AutoCommit.Enable = true
	conf.Consumer.Offsets.Initial = sarama.OffsetOldest

	if IsTruthy(env.GetOrDefault("KAFKA_TLS_ENABLE", "0")) {
		log.Println("Kafka TLS enabled")

		var certBytes []byte
		certBytes, err = base64.StdEncoding.DecodeString(env.GetOrFail("KAFKA_TLS_ROOT_CA"))
		if err != nil {
			return
		}

		tlsConfig(conf, certBytes)
	}

	if IsTruthy(env.GetOrDefault("KAFKA_SASL_ENABLE", "0")) {
		saslType := env.GetOrDefault("KAFKA_SASL_TYPE", "PLAIN")
		conf.Net.SASL.Enable = true
		conf.Net.SASL.Mechanism = sarama.SASLTypePlaintext

		if saslType == "GSSAPI" {
			conf.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI

			authType := env.GetOrDefault("KAFKA_SASL_GSSAPI_AUTH", "")
			if authType == "KEYTAB" {
				log.Println("Kafka SASL GSSAPI auth = keytab")

				var krb5Bytes []byte
				krb5Bytes, err = base64.StdEncoding.DecodeString(env.GetOrFail("KAFKA_SASL_GSSAPI_KRB5CONF"))
				if err != nil {
					return
				}

				username := env.GetOrFail("KAFKA_SASL_GSSAPI_USERNAME")
				realm := env.GetOrFail("KAFKA_SASL_GSSAPI_REALM")

				var keytabBytes []byte
				keytabBytes, err = base64.StdEncoding.DecodeString(env.GetSecretOrFail("KAFKA_SASL_GSSAPI_KEYTAB"))
				if err != nil {
					return
				}

				gssapiKeytabConfig(
					conf,
					krb5Bytes,
					username,
					realm,
					keytabBytes,
				)
			} else if authType == "USER" {
				var krb5Bytes []byte
				krb5Bytes, err = base64.StdEncoding.DecodeString(env.GetOrFail("KAFKA_SASL_GSSAPI_KRB5CONF"))
				if err != nil {
					return
				}

				username := env.GetOrFail("KAFKA_SASL_GSSAPI_USERNAME")
				realm := env.GetOrFail("KAFKA_SASL_GSSAPI_REALM")
				password := env.GetSecretOrFail("KAFKA_SASL_GSSAPI_PASSWORD")

				gssapiUserConfig(
					conf,
					krb5Bytes,
					username,
					realm,
					password,
				)
			}
		}
	}

	addrs = strings.Split(env.GetOrDefault("KAFKA_SERVERS", "localhost:9092"), ",")
	return
}
