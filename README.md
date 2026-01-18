# GeniusBridge

A Py4J-based bridge that allows [Genius](http://ii.tudelft.nl/genius/) negotiation agents to run under [NegMAS](https://github.com/yasserfarouk/negmas) as `GeniusNegotiator` instances.

## Overview

GeniusBridge acts as a Java server that NegMAS connects to via Py4J. It enables running Genius agents (both legacy `Agent`-based and modern `NegotiationParty`-based) within NegMAS negotiation sessions.

## Supported Genius Versions

GeniusBridge supports multiple Genius versions:
- Genius 9.0.0
- Genius 9.1.x
- Genius 10.4

The bridge automatically detects which Genius version is being used and adapts accordingly.

## Installation

### Using NegMAS (Recommended)

The easiest way to install GeniusBridge is through NegMAS:

```bash
pip install negmas
negmas genius-setup
```

This will automatically download and configure GeniusBridge along with the required Genius JAR files.

### Manual Installation

1. Clone this repository
2. Ensure you have JDK 17+ installed
3. Place the required JAR files in the `libs/` directory:
   - `genius.jar` (from your Genius installation)
   - `py4j0.10.8.1.jar` (included)

## Compilation

### Using the Build Script (Recommended)

```bash
./build.sh
```

This creates `out/artifacts/geniusbridge_jar/geniusbridge.jar`.

### Using IntelliJ IDEA

1. Open the project in IntelliJ IDEA
2. Go to **Build > Build Artifacts > geniusbridge:jar > Build**
3. The JAR will be created at `out/artifacts/geniusbridge_jar/geniusbridge.jar`

### Using Command Line

```bash
# Navigate to project root
cd /path/to/geniusbridge

# 1. Create output directories
mkdir -p out/production/geniusbridge
mkdir -p out/artifacts/geniusbridge_jar

# 2. Compile
javac -cp "libs/genius.jar:libs/py4j0.10.8.1.jar" \
      -d out/production/geniusbridge \
      src/com/yasserm/negmasgeniusbridge/Main.java

# 3. Copy resources
cp -r src/com/yasserm/negmasgeniusbridge/resources out/production/geniusbridge/com/yasserm/negmasgeniusbridge/
cp -r src/resources out/production/geniusbridge/

# 4. Create JAR
jar cfm out/artifacts/geniusbridge_jar/geniusbridge.jar \
    src/META-INF/MANIFEST.MF \
    -C out/production/geniusbridge .
```

On Windows, use `;` instead of `:` as the classpath separator:

```bash
javac -cp "libs/genius.jar;libs/py4j0.10.8.1.jar" ...
```

## Deployment

To deploy a new version for NegMAS users (via `negmas genius-setup`):

### Using the Deploy Script

```bash
./deploy.sh
```

This will:
1. Build the JAR using `build.sh`
2. Copy it to the GitHub Pages repository

Then complete the deployment:

```bash
cd /Users/yasser/code/sites/yasserfarouk.github.io
git add output/files/geniusbridge.jar
git commit -m "Update geniusbridge.jar"
git push
```

After pushing, the JAR will be available at:
`https://yasserfarouk.github.io/files/geniusbridge.jar`

## Usage

### Running the Bridge Server

```bash
# Default port (25337)
java -jar geniusbridge.jar

# Custom port
java -jar geniusbridge.jar 25338

# With debug output
java -jar geniusbridge.jar --debug 25337

# Show version
java -jar geniusbridge.jar --version
```

### Command Line Options

| Option | Description |
|--------|-------------|
| `[port]` | Port number (default: 25337) |
| `--debug` | Enable debug mode with verbose logging |
| `--silent` | Suppress all output |
| `--verbose` | Print progress to screen |
| `--timeout=N` | Set timeout in seconds |
| `--force-timeout` | Enforce timeout limits |
| `--no-timeout` | Disable timeout enforcement |
| `--force-timeout-init` | Enforce timeout during agent initialization |
| `--no-timeout-init` | Disable timeout during initialization |
| `--force-timeout-end` | Enforce timeout during negotiation end |
| `--no-timeout-end` | Disable timeout during negotiation end |
| `--with-logs` | Enable logging |
| `--no-logs` | Disable logging |
| `--logfile=FILE` | Specify log file path |
| `--allow-agent-print` | Allow agents to print to console |
| `--die-on-exit` | Kill server when stdin closes (for subprocess management) |

### Using with NegMAS

In Python with NegMAS:

```python
from negmas.genius import GeniusNegotiator

# Create a Genius negotiator
negotiator = GeniusNegotiator(
    java_class_name="agents.anac.y2015.Atlas3.Atlas3",
    domain_file_name="path/to/domain.xml",
    utility_file_name="path/to/utility.xml"
)

# Use in a negotiation session
# ... (see NegMAS documentation for full examples)
```

## Project Structure

```
geniusbridge/
├── src/
│   └── com/yasserm/negmasgeniusbridge/
│       └── Main.java          # Main source file (all classes)
├── libs/
│   ├── genius.jar             # Genius library
│   └── py4j0.10.8.1.jar       # Py4J library
├── out/
│   └── artifacts/
│       └── geniusbridge_jar/
│           └── geniusbridge.jar   # Built JAR
├── domains/                   # Sample negotiation domains
├── build.sh                   # Build script
├── deploy.sh                  # Deploy script
├── README.md
└── LICENSE
```

## Architecture

- **NegLoader**: Main Py4J gateway class that handles all Python-Java communication
- **NegotiatorInfo**: Wrapper holding agent instances and session state
- Supports both:
  - Legacy `Agent` class (bilateral negotiations, pre-2019)
  - Modern `AbstractNegotiationParty` (multilateral negotiations)

## License

This project is licensed under the GNU Affero General Public License v3.0 (AGPL-3.0). See [LICENSE](LICENSE) for details.

## Related Projects

- [NegMAS](https://github.com/yasserfarouk/negmas) - Negotiation Multi-Agent System
- [Genius](http://ii.tudelft.nl/genius/) - General Environment for Negotiation with Intelligent multi-purpose Usage Simulation

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.
