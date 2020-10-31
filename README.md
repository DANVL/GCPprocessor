# GCPprocessor

TO deploy use:

gcloud builds submit --tag gcr.io/apt-index-293821/gcpprocessor
gcloud run deploy --image gcr.io/apt-index-293821/gcpprocessor --platform managed
