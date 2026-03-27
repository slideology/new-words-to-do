import json

from keyword_library import sync_keyword_library


def main():
    payload, artifact_path = sync_keyword_library()
    print(
        json.dumps(
            {
                "artifact_file": str(artifact_path),
                "primary_keyword_count": payload["stats"]["primary_keyword_count"],
                "unique_keyword_count": payload["stats"]["unique_keywords"],
                "rotation_group_count": payload["stats"]["rotation_group_count"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
