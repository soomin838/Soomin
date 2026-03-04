import os

files_to_refactor = [
    "core/settings.py",
    "core/brain.py",
    "core/scout.py",
    "core/quality.py",
    "core/title_variations.py",
    "core/image_prompts.py",
    "core/daily_vector.py",
    "core/ollama_client.py"
]

replacements = [
    ("tech_troubleshoot", "news_interpretation"),
    ("min_external_links_tech_troubleshoot", "min_external_links_news"),
    ("min_authority_links_tech_troubleshoot", "min_authority_links_news"),
    ("disallowed_terms_tech_troubleshoot", "disallowed_terms_news"),
    ("tech_news_only", "news_interpretation"),
    ("troubleshooting checklist", "latest news analysis"),
    ("troubleshooting playbook", "news interpretation guide"),
    ("Software troubleshooting context only", "News and editorial analysis context"),
    ("neutral error indicator", "dynamic editorial illustration"),
    ("neutral error badge", "dynamic news visual"),
    ("troubleshooting process", "news analysis process"),
]

for file_path in files_to_refactor:
    abs_path = os.path.join("c:/Users/soomin/Soomin", file_path)
    if not os.path.exists(abs_path):
        print(f"File not found: {abs_path}")
        continue
    
    with open(abs_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    for old, new in replacements:
        content = content.replace(old, new)
    
    if content != original_content:
        with open(abs_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Updated: {file_path}")
    else:
        print(f"No changes for: {file_path}")
