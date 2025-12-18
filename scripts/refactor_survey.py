import os
import re

TARGET_DIR = "src"

def refactor_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    new_content = content
    
    # Replace imports
    # sipi-etl might have been importing from 'app.db' if it was sharing code, 
    # OR it might be defining its own models.
    # Assuming it needs to import from sipi.db now.
    
    # Common replacements if they were copying models or using a shared path
    new_content = re.sub(r'from app\.db', 'from sipi.db', new_content)
    new_content = re.sub(r'import app\.db', 'import sipi.db', new_content)
    
    # Also check for relative imports that might have been used if code was copied
    
    if content != new_content:
        print(f"Updating {filepath}")
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)

def main():
    print(f"Scanning {TARGET_DIR}...")
    for root, dirs, files in os.walk(TARGET_DIR):
        for file in files:
            if file.endswith(".py"):
                refactor_file(os.path.join(root, file))
    print("Refactoring complete.")

if __name__ == "__main__":
    main()
