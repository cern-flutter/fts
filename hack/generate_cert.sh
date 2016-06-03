#!/bin/bash
#
# Script to create a new host cert/key from the development CA
#

if [ -z "$1" ]; then
    echo "Missing CA directory"
    exit 1
fi
if [ -z "$2" ]; then
    echo "Missing subject name"
    exit 1
fi
if [ -z "$3" ]; then
    echo "Missing output directory"
    exit 1
fi

CADIR=$1
SUBJECT=$2
OUTDIR=$3

pushd "${CADIR}"

mkdir -p "${OUTDIR}"
rm -f "${OUTDIR}/*.pem"

openssl req -config openssl.cnf -batch -new \
    -nodes -keyout "${OUTDIR}/hostkey.pem" \
    -out "${OUTDIR}/hostreq.pem" \
    -subj "${SUBJECT}"

openssl ca -config openssl.cnf -batch \
    -extensions server_cert -days 365 -notext \
    -md sha256 \
    -in "${OUTDIR}/hostreq.pem" \
    -out "${OUTDIR}/hostcert.pem"

popd
