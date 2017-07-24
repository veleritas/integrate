VERSION="neo4j-community-3.1.1"

DB_NAME="rephetio-v2.0"

declare -a suffixes=("" "_perm-1" "_perm-2" "_perm-3" "_perm-4" "_perm-5")

for suffix in "${suffixes[@]}"; do

    BIN_LOC="neo4j/${VERSION}_$DB_NAME$suffix/bin/neo4j-admin"

    CSV_NAME="data/import_csvs/hetnet$suffix"

    REPORT_NAME="network${suffix}_import.report"

    ./$BIN_LOC import --nodes "${CSV_NAME}_nodes.csv" --relationships "${CSV_NAME}_edges.csv" --report-file="$REPORT_NAME"

done
