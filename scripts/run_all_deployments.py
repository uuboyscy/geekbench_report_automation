
import os
import subprocess
import sys
from pathlib import Path


def main():
    # Get the project root directory (assuming this script is in scripts/ folder at project root)
    project_root = Path(__file__).resolve().parent.parent
    deployments_dir = project_root / "src" / "deployments"
    
    if not deployments_dir.exists():
        print(f"Error: Deployments directory not found at {deployments_dir}")
        sys.exit(1)

    # Add src to PYTHONPATH so imports from 'flows' etc. work
    src_dir = project_root / "src"
    env = os.environ.copy()
    # Prepend src to PYTHONPATH
    current_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{src_dir}{os.pathsep}{current_pythonpath}"

    print(f"Looking for deployment scripts in: {deployments_dir}\n")
    
    # Find all python files in deployments dir
    deployment_scripts = sorted(deployments_dir.glob("*.py"))
    
    if not deployment_scripts:
        print("No deployment scripts found.")
        return

    success_count = 0
    failure_count = 0
    total_count = 0

    for script in deployment_scripts:
        # Skip __init__.py if it exists, though typically not run directly
        if script.name == "__init__.py":
            continue
            
        total_count += 1
        print(f"[{total_count}] Running {script.name}...")
        try:
            # Run the script
            # We capture output to avoid clutter unless error, or we can let it stream.
            # Deployment scripts usually print "Deployment 'xyz' created" or similar.
            # Let's let it stream to stdout/stderr so user sees progress in real time.
            subprocess.run(
                [sys.executable, str(script)],
                env=env,
                check=True
            )
            print(f"SUCCESS: {script.name}\n")
            success_count += 1
        except subprocess.CalledProcessError as e:
            print(f"FAILURE: {script.name} exited with code {e.returncode}\n")
            failure_count += 1
        except Exception as e:
            print(f"ERROR: Could not run {script.name}: {e}\n")
            failure_count += 1

    print("-" * 30)
    print(f"Finished running {total_count} deployment scripts.")
    print(f"Successful: {success_count}")
    print(f"Failed:     {failure_count}")

    if failure_count > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()
