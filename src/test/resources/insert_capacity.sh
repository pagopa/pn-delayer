AWS_REGION=us-east-1
AWS_SECRET_ACCESS_KEY=PN-TEST
AWS_ACCESS_KEY_ID=PN-TEST
AWS_ENDPOINT_URL=http://localhost:4566

current_date_iso() {
    date -u +"%Y-%m-%dT%H:%M:%S.%3NZ"
}

DRIVERS=("POSTE" "FULMINE" "SAILPOST" "POSE")

declare -A PROVINCES
if [ -n "$1" ]; then
    # Se il primo parametro è passato, lo usa come lista province (formato: "cap=provincia cap2=provincia2 ...")
    for pair in $1; do
        cap="${pair%%=*}"
        province="${pair#*=}"
        PROVINCES["$cap"]="$province"
    done
else
    # Default
    PROVINCES=(
        ["87100"]="CS"
        ["00118"]="RM"
        ["20121"]="MI"
        ["80122"]="NA"
        ["80121"]="NA"
        ["10121"]="TO"
        ["09131"]="CA"
        ["50121"]="FI"
        ["50122"]="FI"
    )
fi

CAPACITY="${2:-7}"

put_capacity() {
    local table_name="pn-PaperDeliveryDriverCapacities"

    for driver in "${DRIVERS[@]}"; do
        for cap in "${!PROVINCES[@]}"; do
            local province=${PROVINCES[$cap]}

            # Inserimento combinazione driver-cap
            local combination_driver_cap="20250120~${driver}~${cap}"
            echo "Inserimento combinazione: $combination_driver_cap"
            local current_date=$(current_date_iso)
            local date_from=$(date -d "$current_date - 1 day" +"%Y-%m-%dT%H:%M:%S.%3NZ")

            aws dynamodb put-item \
                --table-name "$table_name" \
                --item "{
                    \"pk\": {\"S\": \"${combination_driver_cap}\"},
                    \"unifiedDeliveryDriver\": {\"S\": \"$driver\"},
                    \"geoKey\": {\"S\": \"$cap\"},
                    \"tenderId\": {\"S\": \"20250120\"},
                    \"capacity\": {\"N\": \"$CAPACITY\"},
                    \"peekCapacity\": {\"N\": \"100\"},
                    \"createdAt\": {\"S\": \"$current_date\"},
                    \"activationDateFrom\": {\"S\": \"2025-05-08T00:00:00.000Z\"}
                }" \
                --region "$AWS_REGION" \
                --no-verify-ssl

            if [ $? -eq 0 ]; then
                echo "Combinazione $combination_driver_cap inserita con successo."
            else
                echo "Errore nell'inserimento della combinazione $combination_driver_cap."
            fi

            # Inserimento combinazione driver-province
            local combination_driver_province="20250120~${driver}~${province}"
            echo "Inserimento combinazione: $combination_driver_province"
            local current_date=$(current_date_iso)
            local date_from=$(date -d "$current_date - 1 day" +"%Y-%m-%dT%H:%M:%S.%3NZ")

                  aws dynamodb put-item \
                --table-name "$table_name" \
                --item "{
                    \"pk\": {\"S\": \"${combination_driver_province}\"},
                    \"unifiedDeliveryDriver\": {\"S\": \"$driver\"},
                    \"geoKey\": {\"S\": \"$province\"},
                    \"tenderId\": {\"S\": \"20250120\"},
                    \"capacity\": {\"N\": \"$CAPACITY\"},
                    \"peekCapacity\": {\"N\": \"100\"},
                    \"createdAt\": {\"S\": \"$current_date\"},
                    \"activationDateFrom\": {\"S\": \"2025-05-08T00:00:00.000Z\"}
                }" \
                --region "$AWS_REGION" \
                --no-verify-ssl

            if [ $? -eq 0 ]; then
                echo "Combinazione $combination_driver_province inserita con successo."
            else
                echo "Errore nell'inserimento della combinazione $combination_driver_province."
            fi
        done
    done
}

assume_role() {
    local role_arn=$1
    local response

    response=$(AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY" \
               AWS_SECRET_ACCESS_KEY="$AWS_SECRET_KEY" \
               aws sts assume-role \
               --role-arn "$role_arn" \
               --role-session-name "AssumeRoleSession" \
               --region "$AWS_REGION" \
               --no-verify-ssl \
               2>/dev/null)

    if [ $? -ne 0 ]; then
        echo "Errore durante l'assunzione del ruolo: $response"
        return 1
    fi

    export AWS_ACCESS_KEY_ID=$(echo "$response" | grep -o '"AccessKeyId": "[^"]*"' | cut -d'"' -f4)
    export AWS_SECRET_ACCESS_KEY=$(echo "$response" | grep -o '"SecretAccessKey": "[^"]*"' | cut -d'"' -f4)
    export AWS_SESSION_TOKEN=$(echo "$response" | grep -o '"SessionToken": "[^"]*"' | cut -d'"' -f4)

    echo "Ruolo assunto con successo."
    return 0
}

main() {
  export AWS_CA_BUNDLE=""

    assume_role "$ROLE_ARN"
    if [ $? -ne 0 ]; then
        echo "Impossibile assumere il ruolo IAM. Uscita."
        exit 1
    fi

    put_capacity()

    echo "Operazione completata."
}

main