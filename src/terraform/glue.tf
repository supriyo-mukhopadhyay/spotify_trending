resource "aws_glue_job" "spotify_staging_api" {
  name         = "${var.project}-spotify-staging-job"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${var.spapi_bucket_name}/staging_scripts/get_spotify_new_release_data.py"
    python_version  = 3
  }

  default_arguments = {
    "--enable-job-insights"                 = "true"
    "--job-language"                        = "python"
    "--conf"                                = "spark.rpc.message.maxSize=2000"
    "--enable-metrics"                      = "true"
    "--s3_bucket"                           = var.spapi_bucket_name
    "--target_path"                         = "staging_data/"
    "--additional-python-modules"           = "dotenv, requests,pyspark, pyspark.core"
  }

  timeout = 1500

  number_of_workers = 2
  worker_type       = "G.1X"
}