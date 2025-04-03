import json
import uuid
import random
import csv
from faker import Faker

fake = Faker('it_IT')

def load_cap_city_province_from_csv(csv_filepath):
    data = []
    try:
        with open(csv_filepath, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            next(reader, None)  # Salta l'intestazione, se presente
            for row in reader:
                if len(row) == 3:
                    data.append({
                        'cap': row[0].strip(),
                        'city': row[1].strip(),
                        'province': row[2].strip()
                    })
    except FileNotFoundError:
        print(f"Errore: Il file CSV '{csv_filepath}' non è stato trovato.")
        return []
    except Exception as e:
        print(f"Errore durante la lettura del CSV: {e}")
        return []
    print("found '{}' rows in CSV file".format(len(data)))
    return data

# Specifica il percorso del file CSV
csv_file = 'PagoPA-ListaCLP.csv'
all_csv_data = load_cap_city_province_from_csv(csv_file)
num_csv_rows = len(all_csv_data)

data = []
tender_id = str(uuid.uuid4())
product_types = ["RS", "AR", "890"]
num_elements = 5000
group_size = 500
num_groups = num_elements // group_size
used_provinces = set()
group_province_map = {}

# Forza la stessa provincia per i primi due gruppi
first_province = None
if num_groups >= 2:
    # Scegli una provincia a caso per i primi due gruppi
    first_province_row = random.choice(all_csv_data)
    first_province = first_province_row['province']
    used_provinces.add(first_province) # Considera la prima provincia come usata per evitare riutilizzo non intenzionale

print(f"Provincia forzata per i primi due gruppi: {first_province}")

for i in range(num_groups):
    fixed_province = None
    fixed_cap = None
    current_row = None
    fixed_delivery_driver_id = random.randint(1, 6)

    if i < 2 and first_province:
        fixed_province = first_province
        # Trova righe con la provincia forzata e scegli un CAP diverso per ogni gruppo
        available_rows_for_province = [row for row in all_csv_data if row['province'] == fixed_province]
        if available_rows_for_province:
            # Tenta di scegliere un CAP diverso per i primi due gruppi
            cap_attempts = 0
            max_cap_attempts = 100
            while cap_attempts < max_cap_attempts:
                current_row = random.choice(available_rows_for_province)
                current_cap = current_row['cap']
                # Verifica se è il primo gruppo o se il CAP è diverso dal primo gruppo
                if i == 0 or (i == 1 and current_cap != group_province_map.get(0, {}).get('cap')):
                    fixed_cap = current_cap
                    break
                cap_attempts += 1
            if fixed_cap is None:
                print(f"Avviso: Impossibile trovare un CAP diverso per il gruppo {i+1} con provincia '{fixed_province}'. Potrebbe esserci un solo CAP per questa provincia nel CSV.")
                current_row = random.choice(available_rows_for_province) # Usa un CAP qualsiasi in caso di fallimento
                fixed_cap = current_row['cap']
        else:
            print(f"Errore: Impossibile trovare righe per la provincia forzata '{first_province}'.")
            continue
    else:
        # Trova una provincia non ancora usata per gli altri gruppi
        found_province = False
        max_attempts = 100
        attempts = 0
        while not found_province and attempts < max_attempts:
            attempts += 1
            chosen_row = random.choice(all_csv_data)
            chosen_province = chosen_row['province']
            if chosen_province not in used_provinces:
                fixed_province = chosen_province
                used_provinces.add(chosen_province)
                available_rows_for_province = [row for row in all_csv_data if row['province'] == fixed_province]
                if available_rows_for_province:
                    current_row = random.choice(available_rows_for_province)
                    fixed_cap = current_row['cap']
                    found_province = True
                break
        if not found_province:
            print(f"Errore: Impossibile trovare una provincia diversa per il gruppo {i+1} dopo {max_attempts} tentativi.")
            break

    if fixed_province and fixed_cap and current_row:
        group_province_map[i] = {'province': fixed_province, 'cap': fixed_cap}
        for _ in range(group_size):
            recipient_address_data = {
                'cap': fixed_cap,
                'city': current_row['city'],
                'city2': current_row['city'],
                'pr': fixed_province
            }
            recipient_normalized_address = {
                "fullName": fake.name(),
                "nameRow2": fake.company(),
                "address": fake.street_address(),
                "addressRow2": fake.secondary_address(),
                "cap": recipient_address_data.get('cap', ''),
                "city": recipient_address_data.get('city', ''),
                "city2": recipient_address_data.get('city2', ''),
                "pr": recipient_address_data.get('pr', ''),
                "country": "IT"
            }
            data.append({
                "requestId": str(uuid.uuid4()),
                "iun": str(uuid.uuid4()).replace('-', '').upper()[:15],
                "productType": random.choice(product_types),
                "senderPaId": f"PA{random.randint(10000, 99999)}",
                "deliveryDriverId": fixed_delivery_driver_id,
                "tenderId": tender_id,
                "recipientNormalizedAddress": recipient_normalized_address
            })
    elif i < 2 and not first_province:
        print("Errore: Impossibile determinare la provincia per i primi due gruppi.")
        break

print("\nProvince utilizzate per i gruppi:")
for group_index, province_data in group_province_map.items():
    print(f"Gruppo {group_index + 1}: Provincia - {province_data['province']}, CAP - {province_data['cap']}")

with open('paperDelivery.json', 'w') as f:
    json.dump(data, f, indent=4)

print(f"\nFile 'paperDelivery.json' creato con {len(data)} elementi. I primi due gruppi di {group_size} elementi hanno la stessa provincia ('{first_province}') ma CAP diversi (se disponibili). Tutti gli altri gruppi hanno province diverse.")