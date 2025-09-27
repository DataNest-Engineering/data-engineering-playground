from setuptools import setup
import sys
import traceback

if __name__ == "__main__":
    try:
        setup()
    except Exception as e:
        print("\nError: Project build failed.\n")
        print("Please ensure the following packages are installed and up to date:")
        print("  • setuptools")
        print("  • setuptools_scm")
        print("  • wheel\n")
        print("You can update them using the following command:\n")
        print("  pip install --upgrade setuptools setuptools_scm wheel\n")
        
        print("🔍 Detailed error:")
        traceback.print_exc()
        
        # Exit with non-zero code to indicate failure
        sys.exit(1)
        print("Exiting with code 1.")
        raise e
    