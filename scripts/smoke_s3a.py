"""Prueba de humo S3A: escribe y lee un Parquet en MinIO vía Spark y pandas.

Uso (dentro del contenedor Dagster):
    docker exec uisrael_dagster_webserver \
        python -m scripts.smoke_s3a

Valida que la imagen tiene los JARs de S3A y que las credenciales/endpoint de
MinIO funcionan, antes de repuntar las zonas del pipeline.
"""
from src.processing.config import BRONCE_BUCKET, s3_storage_options
from src.processing.spark_session import get_spark


def main() -> None:
    uri_spark = f"s3a://{BRONCE_BUCKET}/_smoke/spark"
    uri_pandas = f"s3://{BRONCE_BUCKET}/_smoke/pandas.parquet"

    # 1) Spark: write + read
    spark = get_spark("smoke_s3a", cores="1")
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    df.write.mode("overwrite").parquet(uri_spark)
    n = spark.read.parquet(uri_spark).count()
    spark.stop()
    print(f"[SPARK] escritas/leídas {n} filas en {uri_spark}")

    # 2) pandas + s3fs: write + read
    import pandas as pd

    pdf = pd.DataFrame({"id": [10, 20, 30], "val": ["x", "y", "z"]})
    pdf.to_parquet(uri_pandas, index=False, storage_options=s3_storage_options())
    back = pd.read_parquet(uri_pandas, storage_options=s3_storage_options())
    print(f"[PANDAS] escritas/leídas {len(back)} filas en {uri_pandas}")

    assert n == 2 and len(back) == 3, "conteos inesperados"
    print("SMOKE S3A: OK")


if __name__ == "__main__":
    main()
