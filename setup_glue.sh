#!/bin/bash
# CPSC 436C: set up your AWS account for the Glue notebooks (L7 onwards). Run it once, in AWS CloudShell
# (the >_ icon at the top of the AWS console), region Canada (Central):
#
#   aws s3 cp s3://436c-2026w1/l7/student/setup_glue.sh . && bash setup_glue.sh
#
# (also in https://github.com/cpsc436c-2026w1/l7-spark-glue)
#
# It creates, if they don't exist yet:
#   - a bucket for your own results:  436c-glue-<your account number>
#   - a role for Glue notebook sessions: 436c-glue-notebook
#       AWS's managed policy AWSGlueServiceRole (Glue itself, its logs)
#       + read the course data in s3://436c-2026w1/
#       + read and write your bucket
#       + pass itself to a Glue session (a notebook needs this)
# Running it again is safe: it creates only what is missing and re-applies the role's policies. At the end it
# checks that your account can download the course data, and prints what to do next.
set -euo pipefail

REGION=ca-central-1
COURSE_BUCKET=436c-2026w1
ROLE=436c-glue-notebook
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET=436c-glue-$ACCOUNT

echo "Account $ACCOUNT, region $REGION"

# 1. Your bucket
if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "bucket  s3://$BUCKET already exists"
else
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
    --create-bucket-configuration LocationConstraint="$REGION" >/dev/null
  echo "bucket  s3://$BUCKET created"
fi

# 2. The role Glue sessions run as
if aws iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
  echo "role    $ROLE already exists"
else
  aws iam create-role --role-name "$ROLE" --description "CPSC 436C Glue notebook sessions" \
    --assume-role-policy-document '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow",
      "Principal": {"Service": "glue.amazonaws.com"}, "Action": "sts:AssumeRole"}]}' >/dev/null
  echo "role    $ROLE created"
fi
aws iam attach-role-policy --role-name "$ROLE" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
aws iam put-role-policy --role-name "$ROLE" --policy-name course-data-and-own-bucket --policy-document "{
  \"Version\": \"2012-10-17\",
  \"Statement\": [
    {\"Sid\": \"CourseDataList\", \"Effect\": \"Allow\", \"Action\": \"s3:ListBucket\",
     \"Resource\": \"arn:aws:s3:::$COURSE_BUCKET\"},
    {\"Sid\": \"CourseDataRead\", \"Effect\": \"Allow\", \"Action\": \"s3:GetObject\",
     \"Resource\": \"arn:aws:s3:::$COURSE_BUCKET/*\"},
    {\"Sid\": \"OwnBucket\", \"Effect\": \"Allow\",
     \"Action\": [\"s3:ListBucket\", \"s3:GetObject\", \"s3:PutObject\", \"s3:DeleteObject\"],
     \"Resource\": [\"arn:aws:s3:::$BUCKET\", \"arn:aws:s3:::$BUCKET/*\"]},
    {\"Sid\": \"PassItselfToGlue\", \"Effect\": \"Allow\", \"Action\": \"iam:PassRole\",
     \"Resource\": \"arn:aws:iam::$ACCOUNT:role/$ROLE\"}
  ]}"
echo "role    policies attached"

# 3. Check: can this account download the course data? (A real download, not just a listing.)
if aws s3 cp "s3://$COURSE_BUCKET/l7/student/sparkmeter.py" - >/dev/null 2>&1; then
  echo "check   course data readable"
else
  echo ""
  echo "STOP: this account can't download s3://$COURSE_BUCKET/l7/student/sparkmeter.py."
  echo "      Send your account number ($ACCOUNT) to the teaching team; the notebooks won't work until this passes."
  exit 1
fi

cat <<EOF

Done. Next:
  1. On your laptop (not here in CloudShell), download l7a_data-read.ipynb from https://github.com/cpsc436c-2026w1/l7-spark-glue
  2. AWS Glue console > ETL jobs > Notebooks: create a notebook from that file, with the IAM role $ROLE
  3. Run its cells in order: the first ones start a session with 3 workers and load sparkmeter.

To keep your measurements after the session ends, save them to your bucket:

  sm.setup(spark, save_to="s3://$BUCKET/sparkmeter/")

End every session with %stop_session: a session bills until it stops or reaches its idle timeout (15 minutes in the L7 notebook).
EOF
