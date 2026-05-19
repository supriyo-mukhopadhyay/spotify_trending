output "spotify_staging_api" {
  value = aws_glue_job.spotify_staging_api.name
}


output "glue_role" {
  value = aws_iam_role.glue_role.name
}