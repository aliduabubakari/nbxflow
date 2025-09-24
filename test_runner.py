"""Simple test runner for nbxflow examples."""

import sys
import os
from pathlib import Path

# Add the project root to Python path so we can import nbxflow
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✅ Loaded .env file")
except ImportError:
    print("⚠️  python-dotenv not installed. Using system environment variables only.")
    print("   Install with: pip install python-dotenv")

# Test basic import
try:
    import nbxflow
    print(f"✅ nbxflow imported successfully (version {nbxflow.__version__})")
    
    # Show capabilities
    print("\n🔧 Available capabilities:")
    caps = nbxflow.get_capabilities()
    for feature, available in caps.items():
        status = "✅" if available else "❌"
        print(f"   {status} {feature}")
    
except ImportError as e:
    print(f"❌ Failed to import nbxflow: {e}")
    sys.exit(1)

def test_simple_example():
    """Test the simple flow example."""
    print(f"\n{'='*60}")
    print("TESTING: Simple Flow Example")
    print('='*60)
    
    try:
        # Import and run the simple example
        from examples.simple_flow import main
        main()
        print("✅ Simple flow example completed successfully!")
        return True
    except Exception as e:
        print(f"❌ Simple flow example failed: {e}")
        return False

def test_notebook_snippets():
    """Test the notebook snippets."""
    print(f"\n{'='*60}")
    print("TESTING: Notebook Snippets")
    print('='*60)
    
    try:
        # Import notebook snippets (this runs the code)
        import examples.notebook_snippets
        print("✅ Notebook snippets completed successfully!")
        return True
    except Exception as e:
        print(f"❌ Notebook snippets failed: {e}")
        return False

def test_cli():
    """Test CLI functionality."""
    print(f"\n{'='*60}")
    print("TESTING: CLI")
    print('='*60)
    
    try:
        from nbxflow.cli.__main__ import main
        
        # Test help command
        result = main(["--help"])
        print("✅ CLI help command works!")
        
        # Test classify command (doesn't require external dependencies)
        result = main([
            "classify",
            "--name", "test_component",
            "--doc", "This is a test component for data loading",
            "--method", "rules"
        ])
        print("✅ CLI classify command works!")
        
        return True
    except SystemExit:
        # Expected for help command
        return True
    except Exception as e:
        print(f"❌ CLI test failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Starting nbxflow tests...")
    
    # Create output directories
    os.makedirs("examples/output", exist_ok=True)
    os.makedirs(".nbxflow/contracts", exist_ok=True)
    
    # Run tests
    results = []
    results.append(test_simple_example())
    results.append(test_notebook_snippets())
    results.append(test_cli())
    
    # Summary
    passed = sum(results)
    total = len(results)
    
    print(f"\n{'='*60}")
    print(f"TEST SUMMARY: {passed}/{total} tests passed")
    print('='*60)
    
    if passed == total:
        print("🎉 All tests passed! nbxflow is working correctly.")
        
        # Show generated files
        flow_files = list(Path(".").glob("*_flow_*.json"))
        if flow_files:
            print(f"\n📄 Generated FlowSpec files:")
            for f in flow_files:
                print(f"   - {f}")
            
            print(f"\n💡 Try these commands:")
            latest = flow_files[-1]
            print(f"   python -m nbxflow lineage --flow-json {latest} --format ascii")
            print(f"   python -m nbxflow export --flow-json {latest} --to airflow --out test_dag.py")
    else:
        print(f"⚠️  {total - passed} tests failed. Check the output above.")
        sys.exit(1)