import pyspark
import pytest
from delta import configure_spark_with_delta_pip


@pytest.fixture()
def spark(tmpdir):
    # Configurar a SparkSession com Delta Lake e metastore embutido
    builder = (
        pyspark.sql.SparkSession.builder.appName("MyApp")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", tmpdir)
        .config("spark.ui.enabled", False)
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
