"""Add tls_server_uri to s3 config

Revision ID: 9c11f6c6f152
Revises: fee786dfe121
Create Date: 2021-12-22 12:59:17.737066+00:00

"""
import re

from alembic import op
import sqlalchemy as sa
from cryptography import x509
from cryptography.x509.oid import ExtensionOID, NameOID

# revision identifiers, used by Alembic.
revision = '9c11f6c6f152'
down_revision = 'fee786dfe121'
branch_labels = None
depends_on = None

# Pattern is taken from middlewared.validators.Hostname
hostname_re = re.compile(r'^[a-z\.\-0-9]*[a-z0-9]$', flags=re.IGNORECASE)


def is_valid_hostname(hostname: str):
    """
    Validates hostname and makes sure it
    does not contain a wild card.
    """
    return hostname_re.match(hostname)


def upgrade():
    with op.batch_alter_table('services_s3', schema=None) as batch_op:
        batch_op.add_column(sa.Column('s3_tls_server_uri', sa.String(length=128), nullable=True))

    # Try to get tls_server_uri in following order:
    # 1. SAN from certificate
    # 2. Common name from certificate
    # 3. Fallback to localhost
    conn = op.get_bind()
    if s3_conf := conn.execute("SELECT s3_certificate_id FROM services_s3 WHERE s3_certificate_id IS NOT NULL").fetchone():
        if cert_data := conn.execute("SELECT cert_certificate FROM system_certificate WHERE id = :cert_id", cert_id=s3_conf[0]).fetchone():
            s3_tls_server_uri = 'localhost'
            try:
                cert = x509.load_pem_x509_certificate(cert_data[0].encode())
                cert_cn = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
                if cert_cn and is_valid_hostname(cert_cn[0].value):
                    s3_tls_server_uri = cert_cn[0].value

                cert_sans = []
                try:
                    san_ext = cert.extensions.get_extension_for_oid(ExtensionOID.SUBJECT_ALTERNATIVE_NAME)
                except x509.ExtensionNotFound:
                    san_ext = None
                if san_ext:
                    cert_sans = [str(getattr(entry, 'value', entry)) for entry in san_ext.value]

                for cert_san in cert_sans:
                    san = cert_san.split(':')[-1].strip()
                    if san and is_valid_hostname(san):
                        s3_tls_server_uri = san
                        break
            except Exception:
                pass

            conn.execute(
                "UPDATE services_s3 SET s3_tls_server_uri = :s3_tls_server_uri",
                s3_tls_server_uri=s3_tls_server_uri
            )


def downgrade():
    with op.batch_alter_table('services_s3', schema=None) as batch_op:
        batch_op.drop_column('s3_tls_server_uri')
