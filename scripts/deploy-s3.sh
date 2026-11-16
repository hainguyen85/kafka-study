#!/usr/bin/env bash
# Upload portal + bài học lên bucket S3 static website.
# Cấu hình (không chứa secret) nằm trong deploy/s3.env, do scripts/wizard-s3-portal.sh tạo.
# Credential nằm trong ~/.aws của WSL (aws configure --profile ...), không nằm trong repo.
#
# Chạy trong WSL:
#   bash scripts/deploy-s3.sh            # đồng bộ, xoá trên S3 những file đã xoá ở repo
#   bash scripts/deploy-s3.sh --dryrun   # chỉ in ra sẽ upload/xoá gì
#
# Trước khi deploy, build lại nội dung (trên Windows hoặc nơi có python + node):
#   python scripts/html2md.py && node scripts/build_portal.mjs
set -euo pipefail
cd "$(dirname "$0")/.."

[[ -f deploy/s3.env ]] || { echo "Thiếu deploy/s3.env: chạy bash scripts/wizard-s3-portal.sh trước." >&2; exit 1; }
# shellcheck disable=SC1091
source deploy/s3.env
: "${S3_BUCKET:?thiếu S3_BUCKET trong deploy/s3.env}"
: "${S3_REGION:?thiếu S3_REGION trong deploy/s3.env}"
: "${S3_PROFILE:?thiếu S3_PROFILE trong deploy/s3.env}"
command -v aws >/dev/null 2>&1 || { echo "Chưa có AWS CLI: chạy wizard (stage 2)." >&2; exit 1; }
[[ -f portal/course.js ]] || { echo "Thiếu portal/course.js: chạy node scripts/build_portal.mjs." >&2; exit 1; }

# Endpoint website: một số region cũ dùng dấu gạch, còn lại dùng dấu chấm
# (bảng "Amazon S3 website endpoints", https://docs.aws.amazon.com/general/latest/gr/s3.html).
website_url() {
  case "$1" in
    us-east-1|us-west-1|us-west-2|ap-southeast-1|ap-southeast-2|ap-northeast-1|eu-west-1|sa-east-1|us-gov-west-1)
      echo "http://$2.s3-website-$1.amazonaws.com" ;;
    *) echo "http://$2.s3-website.$1.amazonaws.com" ;;
  esac
}

aws s3 sync . "s3://$S3_BUCKET" \
  --profile "$S3_PROFILE" --region "$S3_REGION" --delete \
  --exclude ".git/*" --exclude ".claude/*" --exclude ".agents/*" --exclude ".vscode/*" \
  --exclude "dist/*" --exclude "deploy/*" --exclude "*/target/*" --exclude "*.local.json" \
  --cache-control "max-age=300" \
  "$@"

echo
echo "Website: $(website_url "$S3_REGION" "$S3_BUCKET")/"
