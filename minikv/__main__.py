"""Allow 'python -m minikv'."""

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
