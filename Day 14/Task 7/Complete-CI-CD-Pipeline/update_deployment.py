import re
import sys

new_tag = sys.argv[1]
file_path = "deployment.yaml"

with open(file_path, "r") as file:
    content = file.read()

# Use a lambda function to safely insert the new tag
pattern = r"(image:\s+docker\.io/.+?/my-cicd-app:)([^\s]+)"
new_content = re.sub(pattern, lambda m: f"{m.group(1)}{new_tag}", content)

with open(file_path, "w") as file:
    file.write(new_content)

print(f"✅ Updated deployment.yaml with image tag: {new_tag}")
#checking if updates made to image tag