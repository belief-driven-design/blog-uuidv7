# UUIDv7 in Java: Time-Ordered IDs for a Modern World

This is the accompanying code of my blog article:

*   **Medium**: <https://medium.com/@benweidig/uuidv7-in-java-b7124502729d>
*   **Blog**: <https://belief-driven-design.com/3ccbb>

The article provides an in-depth explanation of:

*   Why UUIDv4 is problematic for databases
*   How UUIDv7 solves these problems
*   Step-by-step implementation details
*   The lock-free CAS algorithm
*   Bit-packing strategies

## Gradle tasks

```bash
# Run tests
./gradlew test

# Generate single UUIDv7
./gradlew run
```

## Specification

- **RFC**: [RFC 9562 - UUIDs](https://datatracker.ietf.org/doc/html/rfc9562)
- **Java Version**: 21+
- **Dependencies**: None (pure JDK)

## License

[CC0 1.0 Universal](https://creativecommons.org/publicdomain/zero/1.0/) - Public Domain

To the extent possible under law, the author has waived all copyright and related or neighboring rights to this work.
You can copy, modify, distribute and perform the work, even for commercial purposes, all without asking permission.
