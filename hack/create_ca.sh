#!/bin/bash
#
# Script to create a development CA
#
set -x

CA_SUBJECT="/DC=ch/DC=cern/OU=devel/CN=Devel CA"

if [ -z "$1" ]; then
    echo "Missing output directory"
    exit 1
fi

CADIR=$1

# Prepare destination directory
mkdir -p "${CADIR}"
pushd "${CADIR}"

# Prepare config files and directories
mkdir -p certs crl newcerts private
touch index.txt
echo "unique_subject = no" > index.txt.attr
echo 42 > serial

cat > openssl.cnf <<EOF
[ca]
default_ca = CA_DEV

[CA_DEV]
dir   = ${CADIR}
certs = ${CADIR}/certs
new_certs_dir = ${CADIR}/newcerts
database = ${CADIR}/index.txt
serial = ${CADIR}/serial

private_key = ${CADIR}/private/cakey.pem
certificate = ${CADIR}/certs/cacert.pem

default_md = sha256
policy = policy_loose

[policy_loose]
countryName             = optional
stateOrProvinceName     = optional
organizationName        = optional
organizationalUnitName  = optional
commonName              = supplied
emailAddress            = optional

[req]
default_bits        = 2048
distinguished_name  = req_distinguished_name
string_mask         = utf8only
default_md          = sha256
x509_extensions     = v3_ca

[req_distinguished_name]
countryName         = Country Name (2 letter code)
countryName_default = CH
countryName_min     = 2
countryName_max     = 2

stateOrProvinceName     = State or Province Name (full name)
stateOrProvinceName_default = Geneva

localityName            = Locality Name (eg, city)

0.organizationName      = Organization Name (eg, company)
0.organizationName_default  = CERN

organizationalUnitName      = Organizational Unit Name (eg, section)
organizationalUnitName_default = DEV

commonName          = Common Name (e.g. server FQDN or YOUR name)
commonName_max          = 64

emailAddress            = Email Address
emailAddress_max        = 64

[v3_ca]
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
basicConstraints = critical, CA:true
keyUsage = critical, digitalSignature, cRLSign, keyCertSign

[server_cert]
basicConstraints = CA:FALSE
nsCertType = server
nsComment = "DEV CERTIFICATE"
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer:always
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
EOF

# Generate private key and CA certificate
if [ ! -f "private/cakey.pem" ]; then
    openssl req -config openssl.cnf -batch \
        -new -nodes -x509 -sha256 \
        -days 1825 \
        -extensions v3_ca \
        -keyout private/cakey.pem \
        -out certs/cacert.pem \
        -subj "${CA_SUBJECT}"

    chmod 0400 private/cakey.pem
fi

popd
