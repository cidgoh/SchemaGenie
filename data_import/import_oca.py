import os
import json
import uuid
import argparse
from neo4j import GraphDatabase
from dotenv import load_dotenv

# -------------------------------
# Load environment variables
# -------------------------------
load_dotenv()  # Load from .env file

# -------------------------------
# Configuration
# -------------------------------
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# -------------------------------
# Parse command line arguments
# -------------------------------
def parse_arguments():
    parser = argparse.ArgumentParser(description='Import OCA schemas into Neo4j')
    parser.add_argument(
        '--folder', 
        '-f', 
        type=str, 
        default="./schemas",
        help='Path to the folder containing JSON schema files (default: ./schemas)'
    )
    return parser.parse_args()

# -------------------------------
# Function to import one JSON schema
# -------------------------------
def import_oca_package(json_file):
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    schema_name = os.path.basename(json_file).replace(".json", "")
    print(f"Processing: {schema_name}")

    # -------------------------------
    # Access OCA bundle structure
    # -------------------------------
    oca_bundle = data.get("oca_bundle", {})
    bundle = oca_bundle.get("bundle", {})
    capture_base = bundle.get("capture_base", {})

    # IDs (use fallback UUID if schema_id is missing)
    schema_id = data.get("d") or str(uuid.uuid4())
    capture_base_id = capture_base.get("d")

    # -------------------------------
    # Attributes
    # -------------------------------
    attributes = capture_base.get("attributes", {})

    # -------------------------------
    # Overlays
    # -------------------------------
    overlays = bundle.get("overlays", {})

    # ---- Units
    unit_overlay = overlays.get("unit", {})
    units = unit_overlay.get("attribute_unit", {}) if unit_overlay else {}

    # ---- Information (bilingual)
    information_overlays = overlays.get("information", [])
    attribute_descriptions = {}
    for info in information_overlays:
        lang = info.get("language", "unknown")
        attr_info = info.get("attribute_information", {})
        for attr, desc in attr_info.items():
            attribute_descriptions.setdefault(attr, {})[lang] = desc

    # ---- Entry overlays
    entry_overlays = overlays.get("entry", [])
    controlled_vocabularies = {}
    for entry in entry_overlays:
        lang = entry.get("language", "unknown")
        attr_entries = entry.get("attribute_entries", {})
        for attr, entries in attr_entries.items():
            controlled_vocabularies.setdefault(attr, {})[lang] = entries

    # ---- Entry code overlays
    entry_code_overlay = overlays.get("entry_code", {})
    entry_codes = entry_code_overlay.get("attribute_entry_codes", {}) if entry_code_overlay else {}

    # ---- Format overlay
    format_overlay = overlays.get("format", {})
    attribute_formats = format_overlay.get("attribute_formats", {}) if format_overlay else {}

    # ---- Meta overlays
    meta_overlays = overlays.get("meta", [])

    # -------------------------------
    # Extensions (ordering)
    # -------------------------------
    extensions = data.get("extensions", {})
    adc_extension = extensions.get("adc", {})
    ordering_data = {}
    if capture_base_id in adc_extension:
        ordering_overlays = adc_extension[capture_base_id].get("overlays", {})
        ordering = ordering_overlays.get("ordering", {})
        ordering_data = {
            "attribute_ordering": ordering.get("attribute_ordering", []),
            "entry_code_ordering": ordering.get("entry_code_ordering", {})
        }

    # -------------------------------
    # Insert into Neo4j
    # -------------------------------
    with driver.session() as session:
        # Schema node
        session.run(
            """
            MERGE (s:Schema {id: $schema_id})
            SET s.name = $schema_name,
                s.displayName = $schema_name,
                s.capture_base_id = $capture_base_id,
                s.type = $schema_type
            """,
            schema_id=schema_id,
            schema_name=schema_name,
            capture_base_id=capture_base_id,
            schema_type=data.get("type", "oca_package/1.0")
        )

        # Attributes
        for attr_name, attr_type in attributes.items():
            attr_unit = units.get(attr_name)
            attr_description = json.dumps(attribute_descriptions.get(attr_name)) if attribute_descriptions.get(attr_name) else None
            attr_format = attribute_formats.get(attr_name)
            attr_vocabulary = controlled_vocabularies.get(attr_name)
            attr_codes = entry_codes.get(attr_name)

            session.run(
                """
                MERGE (a:Attribute {name: $attr_name})
                SET a.type = $attr_type,
                    a.unit = $attr_unit,
                    a.description = $attr_description,
                    a.format = $attr_format,
                    a.vocabulary = $attr_vocabulary,
                    a.codes = $attr_codes
                WITH a
                MATCH (s:Schema {id: $schema_id})
                MERGE (s)-[:HAS_ATTRIBUTE]->(a)
                """,
                attr_name=attr_name,
                attr_type=attr_type,
                attr_unit=attr_unit,
                attr_description=attr_description,
                attr_format=attr_format,
                attr_vocabulary=json.dumps(attr_vocabulary) if attr_vocabulary else None,
                attr_codes=json.dumps(attr_codes) if attr_codes else None,
                schema_id=schema_id
            )

        # Meta overlays
        for meta in meta_overlays:
            meta_name = meta.get("name")
            meta_desc = meta.get("description")
            meta_language = meta.get("language", "unknown")
            if meta_name:
                session.run(
                    """
                    MERGE (m:Meta {name: $meta_name, schema_id: $schema_id, language: $meta_language})
                    SET m.description = $meta_desc
                    WITH m
                    MATCH (s:Schema {id: $schema_id})
                    MERGE (s)-[:HAS_META]->(m)
                    """,
                    meta_name=meta_name,
                    meta_desc=meta_desc,
                    meta_language=meta_language,
                    schema_id=schema_id
                )

        # Ordering
        if ordering_data:
            session.run(
                """
                MATCH (s:Schema {id: $schema_id})
                SET s.attribute_ordering = $attribute_ordering,
                    s.entry_code_ordering = $entry_code_ordering
                """,
                schema_id=schema_id,
                attribute_ordering=ordering_data.get("attribute_ordering", []),
                entry_code_ordering=json.dumps(ordering_data.get("entry_code_ordering", {}))
            )

# -------------------------------
# Main execution
# -------------------------------
def main():
    args = parse_arguments()
    schema_folder = args.folder

    # Validate folder exists
    if not os.path.exists(schema_folder):
        print(f"❌ Error: Folder '{schema_folder}' does not exist!")
        return
    
    if not os.path.isdir(schema_folder):
        print(f"❌ Error: '{schema_folder}' is not a directory!")
        return

    print(f"📁 Scanning folder: {schema_folder}")

    # Connect to Neo4j
    global driver
    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD)
    )

    # Process all JSON files
    json_files = [f for f in os.listdir(schema_folder) if f.endswith(".json")]
    
    if not json_files:
        print(f"❌ No JSON files found in '{schema_folder}'!")
        driver.close()
        return

    print(f"📄 Found {len(json_files)} JSON file(s) to process")

    for file_name in json_files:
        file_path = os.path.join(schema_folder, file_name)
        try:
            import_oca_package(file_path)
        except Exception as e:
            print(f"❌ Error importing {file_name}: {e}")

    print("✅ All schemas imported successfully!")
    driver.close()

if __name__ == "__main__":
    main()