set -e 

TAXI=$1  #green / yellow
YEAR=$2 # 2021,2020


URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data"

for MONTH in {1..12}; do
  FMONTH=`printf "%02d" ${MONTH}`
#   CURR_DIR=`pwd`

  LOCAL_PREFIX="data/raw/${TAXI}/${YEAR}/${MONTH}"
  LOCAL_FILE="${TAXI}_tripdata_${YEAR}_${FMONTH}.parquet"
  LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

  URL="${URL_PREFIX}/${TAXI}_tripdata_${YEAR}-${FMONTH}.parquet"

  mkdir -p ${LOCAL_PREFIX}
  wget ${URL} -O ${LOCAL_PATH}
done 

