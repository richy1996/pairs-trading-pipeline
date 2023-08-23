# Script for deploying a Cloud Build job that:
  # uploads a gzipped version of the R script and Dockerfile in build directory to GCS, used as source code for Build
  # creates a cloudbuild.yaml configuration file with the following steps:
    # get credentials from Google Secret Manager for a service account that can perform BigQuery operations
    # create and push to cloud container repository a Docker image from the Dockerfile in the source
    # run the R script calculate_lasso.R against a Docker container created from that image
  # submits the job to Cloud Build and opens the job log in a browser

# Requirements:
  # gsutil cli tool
  # previous setup and auth of googleCloudRunner to operate under your username and in the appropriate GCP project
  # a service account permissioned to run BigQuery operations, keys stored in Google Secret Manager
  # configure the user-defined parameters below

library(googleCloudRunner)
library(glue)

# User-defined parameters
PATH <- "equity-stat-arb/pairs-trading-pipeline/graphical_lasso"  # local project directory 
BUCKET <- "your-gcs-bucket"  # name of your GCS bucket to deploy source
SECRET_NAME <- "name-of-your-bq-authed-secret"  # keys for a service account permissioned to operate on BigQuery

cloudbuild_file <- glue("{PATH}/cloudbuild.yml")

# make tar.gz file from build dir and push to cloud
# if specifying a tar.gz as source for build, will get unzipped automatially
system2("tar", args = c("-C", PATH, "-czvf", glue("{PATH}/build.tar.gz"), "build"))
system2("gsutil", args = c("cp", glue("{PATH}/build.tar.gz"), glue("gs://{BUCKET}")))

bs <- c(
  cr_buildstep_secret(
    # download encrypted files from Google Secret Manager for use in build
    SECRET_NAME,  # name of the secret in Secret Manager
    decrypted = "build/auth.json"   # file to decrypt secret into
  ),
  cr_buildstep_docker(
    # build and push a Docker image
    "make-lasso-feature",  # this image will be available as "gcr.io/$PROJECT_ID/make-lasso-feature:$BUILD_ID"
    location = "build", # directory to get the Docker image from 
    kaniko_cache = TRUE  # cache to speed up subsequent builds
  ),
  cr_buildstep_r(
    # run R code within the build
    "build/calculate_lasso.R",  # location of R script within source
    r_source = "runtime",  # use 'runtime' if file comes from within source, use 'local' if copying over at build time by specifying local file or gcs file
    name = "gcr.io/$PROJECT_ID/make-lasso-feature:$BUILD_ID", # run R script against previously created Docker container
    env = "BQ_AUTH_FILE=auth.json,BQ_DEFAULT_PROJECT_ID=$PROJECT_ID"  # environment variables
  )
)

# construct cloudbuild.yaml. note using a higher powered instance and a longer-than-default timeout.
by <- cr_build_yaml(bs, timeout = 1800, options = list("machineType" = 'E2_HIGHCPU_32'))
cr_build_write(by, file = cloudbuild_file)

# build manually with source from cloud storage
src <- cr_build_source(StorageSource(object = "build.tar.gz", BUCKET = BUCKET))
cr_build(cloudbuild_file, source = src)