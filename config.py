"""Workspace configuration.

Package definitions for flat project structure.
"""

from collections.abc import Iterator
from pathlib import Path

from meta.recipe import Recipe, UploadEndpoint
from meta import versioning as v


def Recipes(pattern: str = "*") -> Iterator[Recipe]:
    """Yield configured ``Recipe`` objects filtered by the optional pattern."""
    for path in Path().glob(f"{pattern}/**/recipe.yaml"):
        package_name = path.parts[0]
        recipe = Recipe(
            path=path,
            upload_endpoint=UploadEndpoint.from_url("prefix://meta-forge"),
            runner=["gha-runner-scale-set"],
            conda_channels=["https://prefix.dev/meta-forge", "conda-forge"],
        )

        match package_name:
            case "aider" | "podman-rootful":
                recipe.update_pipeline = v.skip()
            case "qemu" | "sctp" | "nvidia-shim":
                recipe.update_pipeline = v.github_tags(version_like=True)
            case "passt" | "systemtap" | "tomcli":
                recipe.update_pipeline = v.github_tags()
            case "betterproto2":
                recipe.update_pipeline = v.github_tags(
                    version_like=True,
                    exclude_prefix=["compiler-", "v"],
                )
            case "sgl-kernel" | "betterproto" | "decord":
                recipe.update_pipeline = v.git_commit()
            case "deepspeed" | "transformer-engine":
                recipe.runner = ["very-large-runner"]

        yield recipe


if __name__ == "__main__":
    for r in Recipes():
        print(r)
