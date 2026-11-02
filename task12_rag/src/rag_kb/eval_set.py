OUT_OF_SCOPE = "The Delta Lake documentation does not cover this."

QUESTIONS = [
    {
        "id": "q01",
        "question": "What is the default retention threshold for files removed by VACUUM?",
        "doc": "delta-utility/index.mdx",
        "reference": "7 days.",
    },
    {
        "id": "q02",
        "question": "How long is table history kept by default, and which table property controls it?",
        "doc": "delta-batch.mdx",
        "reference": "delta.logRetentionDuration, default interval 30 days.",
    },
    {
        "id": "q03",
        "question": "Which table property enables deletion vectors on a Delta table?",
        "doc": "delta-deletion-vectors.mdx",
        "reference": "Set 'delta.enableDeletionVectors' = true with ALTER TABLE ... SET TBLPROPERTIES.",
    },
    {
        "id": "q04",
        "question": "In which Delta Lake version did MERGE start using deletion vectors?",
        "doc": "delta-deletion-vectors.mdx",
        "reference": "Delta Lake 3.1.0, where it is also enabled by default.",
    },
    {
        "id": "q05",
        "question": "Where does Delta Lake store change data for UPDATE, DELETE and MERGE when change data feed is enabled?",
        "doc": "delta-change-data-feed.mdx",
        "reference": "In the _change_data folder under the table directory; it follows the table retention policy.",
    },
    {
        "id": "q06",
        "question": "What is the default value of maxFilesPerTrigger when Delta is used as a streaming source?",
        "doc": "delta-streaming/index.mdx",
        "reference": "1000 files per micro-batch.",
    },
    {
        "id": "q07",
        "question": "Which streaming option is recommended to skip commits that update or delete data in a Delta source?",
        "doc": "delta-streaming/index.mdx",
        "reference": "skipChangeCommits; ignoreChanges is deprecated in its favour.",
    },
    {
        "id": "q08",
        "question": "How many clustering columns can a table with liquid clustering have?",
        "doc": "delta-clustering.mdx",
        "reference": "Up to 4 clustering columns.",
    },
    {
        "id": "q09",
        "question": "Which table property limits statistics collection to the first N columns?",
        "doc": "delta-batch.mdx",
        "reference": "delta.dataSkippingNumIndexedCols.",
    },
    {
        "id": "q10",
        "question": "What value of delta.columnMapping.mode enables column mapping?",
        "doc": "delta-column-mapping.mdx",
        "reference": "'name', with reader version 2 and writer version 5 or above.",
    },
    {
        "id": "q11",
        "question": "From which Delta version can row tracking be enabled on an existing table?",
        "doc": "delta-row-tracking.mdx",
        "reference": "Delta 3.3 and above, with ALTER TABLE ... SET TBLPROPERTIES ('delta.enableRowTracking' = 'true').",
    },
    {
        "id": "q12",
        "question": "With type widening, what is the minimum decimal type an int column can be widened to?",
        "doc": "delta-type-widening.mdx",
        "reference": "decimal(10,0); for long it is decimal(20,0). Integer to decimal or double changes must be applied manually.",
    },
    {
        "id": "q13",
        "question": "Which table property enables default column values?",
        "doc": "delta-default-columns.mdx",
        "reference": "Set 'delta.feature.allowColumnDefaults' = 'enabled'.",
    },
    {
        "id": "q14",
        "question": "In the default S3 mode, where must concurrent writes to a Delta table come from?",
        "doc": "delta-storage.mdx",
        "reference": "From a single Spark driver; multi-cluster writes need the multi-cluster setup.",
    },
    {
        "id": "q15",
        "question": "Which Spark SQL extension must be configured to use Delta Lake with Apache Spark?",
        "doc": "quick-start.mdx",
        "reference": "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension, plus the DeltaCatalog as spark_catalog.",
    },
    {
        "id": "q16",
        "question": "How do I create a vector index on a Delta table with the CREATE VECTOR INDEX command?",
        "doc": None,
        "reference": OUT_OF_SCOPE,
    },
    {
        "id": "q17",
        "question": "What is the price per terabyte of Delta Lake storage?",
        "doc": None,
        "reference": OUT_OF_SCOPE,
    },
    {
        "id": "q18",
        "question": "Which Delta Lake version added native support for Avro data files instead of Parquet?",
        "doc": None,
        "reference": OUT_OF_SCOPE,
    },
]
