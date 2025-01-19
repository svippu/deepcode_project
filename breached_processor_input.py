import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import urlparse
import json
import os
import dns.resolver

# Database connection details
DATABASE_URL = "postgresql://breachdb_owner:H6csGDI9Wkqw@ep-plain-haze-a8r6y8j7.eastus2.azure.neon.tech/breachdb"
engine = create_engine(DATABASE_URL)


# Resolve domain to IP address
def resolve_domain(domain):
    try:
        answers = dns.resolver.resolve(domain, 'A')  # Query A records for the domain
        for rdata in answers:
            return str(rdata)  # Return the first IP address
    except dns.resolver.NXDOMAIN:
        print(f"Domain does not exist: {domain}")
    except dns.resolver.NoAnswer:
        print(f"No answer found for domain: {domain}")
    except Exception as e:
        print(f"Error resolving domain {domain}: {e}")
    return None


# Function to handle ports safely
def safe_port(parsed_uri):
    try:
        return int(parsed_uri.port)  # Try converting to integer
    except (ValueError, TypeError):
        # Return default ports based on the scheme if invalid
        if parsed_uri.scheme == "https":
            return 443
        elif parsed_uri.scheme == "http":
            return 80
        else:
            return None


# Resolve domain to IP address
def resolve_domain(domain):
    try:
        answers = dns.resolver.resolve(domain, 'A')  # Query A records for the domain
        for rdata in answers:
            return str(rdata)  # Return the first IP address
    except dns.resolver.NXDOMAIN:
        print(f"Domain does not exist: {domain}")
    except dns.resolver.NoAnswer:
        print(f"No answer found for domain: {domain}")
    except Exception as e:
        print(f"Error resolving domain {domain}: {e}")
    return None



def enrich_data(row):
    tags = []  # Initialize tags list

    # Resolve domain to IP address
    try:
        if row.get("domain"):
            ip_address = resolve_domain(row["domain"])
            row["ip_address"] = ip_address
            if ip_address:
                tags.append("resolved")
            else:
                tags.append("unresolved")
        else:
            row["ip_address"] = None
            tags.append("unresolved")
    except Exception as e:
        print(f"Error resolving domain for row {row}: {e}")
        row["ip_address"] = None
        tags.append("unresolved")

    # Add tags as JSON-compatible string
    row["tags"] = json.dumps(tags)
    return row

def parse_and_enrich(file_path):
    # Parse the file
    parsed_df = parse_sample_file(file_path)

    # Enrich the data
    enriched_df = parsed_df.apply(enrich_data, axis=1)

    return enriched_df


# Parse the input file
def parse_sample_file(file_path):
    parsed_data = []
    with open(file_path, 'r', encoding='utf-8', errors='replace') as infile:
        for line in infile:
            # Split the line into parts from the end
            parts = line.rsplit(":", 2)  # Split at most twice from the right
            if len(parts) == 3:
                uri, username, password = parts
                parsed_uri = urlparse(uri)

                # Append the parsed data
                parsed_data.append({
                    "uri": uri.strip(),
                    "username": username.strip(),
                    "password": password.strip(),
                    "domain": parsed_uri.netloc,
                    "port": safe_port(parsed_uri),  # Use safe_port to handle invalid ports
                    "path": parsed_uri.path.strip(),
                })
            else:
                print(f"Skipping malformed line: {line.strip()}")
    return pd.DataFrame(parsed_data)

# Store data in PostgreSQL
def store_data_to_db(data):
    data.to_sql('breach_data', engine, if_exists='append', index=False)
    print("Data stored successfully.")

# Create the table in PostgreSQL
def create_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS breach_data (
        id SERIAL PRIMARY KEY,
        uri TEXT NOT NULL,
        domain TEXT,
        ip_address VARCHAR(45),
        port INT,
        path TEXT,
        username VARCHAR(25),
        password VARCHAR(25),
        tags JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        connection = psycopg2.connect(DATABASE_URL)
        cursor = connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS breach_data;")  # Drop table if exists
        cursor.execute(create_table_query)  # Create the table
        connection.commit()
        print("Table `breach_data` created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
def add_indexes():
    index_queries = [
        "CREATE INDEX IF NOT EXISTS idx_domain ON breach_data(domain);",
        "CREATE INDEX IF NOT EXISTS idx_ip_address ON breach_data(ip_address);",
        "CREATE INDEX IF NOT EXISTS idx_port ON breach_data(port);",
        "CREATE INDEX IF NOT EXISTS idx_path ON breach_data(path);",
        "CREATE INDEX IF NOT EXISTS idx_username ON breach_data(username);",
        "CREATE INDEX IF NOT EXISTS idx_tags ON breach_data USING gin(tags);"
    ]

    try:
        connection = psycopg2.connect(DATABASE_URL)
        cursor = connection.cursor()

        for query in index_queries:
            cursor.execute(query)
            print(f"Executed: {query}")

        connection.commit()
        print("Indexes added successfully.")
    except Exception as e:
        print(f"Error adding indexes: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Main execution
if __name__ == "__main__":
    # Create the table
    create_table()
    add_indexes()
    # Input file path
    input_file = "sample2.txt"  # Replace with the actual file path
    if not os.path.exists(input_file):
        print(f"Error: File {input_file} does not exist.")
    else:
        # Parse the file
        parsed_df = parse_and_enrich(input_file)
        print(f"Parsed {len(parsed_df)} entries from the file.")

        # Store data in the database
        store_data_to_db(parsed_df)
        print("All data stored successfully.")
