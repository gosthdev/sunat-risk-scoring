import pytest
from s3_paths import staging_de


@pytest.mark.parametrize(
    "viva, esperada",
    [
        ("s3://b-gold/ruc_features", "s3://b-gold/_staging/ruc_features"),
        ("s3://uno/gold/ruc_features", "s3://uno/_staging/gold/ruc_features"),
    ],
)
def test_staging_de(viva, esperada):
    assert staging_de(viva) == esperada
