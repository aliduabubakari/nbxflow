"""Prompts for LLM-assisted operations."""

CLASSIFY_COMPONENT_PROMPT = """You are a data pipeline architect. Your task is to classify a data pipeline component into one of these types:

Component Types:
- DataLoader: Loads data from files, databases, or APIs into the pipeline
- Transformer: Transforms, cleans, or processes data without adding external information
- Reconciliator: Matches, deduplicates, or reconciles data entities (often using external services)
- Enricher: Adds new information to existing data using external APIs or datasets
- Exporter: Outputs or saves data to external systems, files, or databases
- QualityCheck: Validates data quality, runs tests, or checks data contracts
- Splitter: Splits datasets into multiple outputs based on criteria
- Merger: Combines multiple datasets into a single output
- Orchestrator: Coordinates other components or manages workflow execution
- Other: Components that don't fit the above categories

Classification Guidelines:
1. Primary Function: What is the main purpose of this component?
2. Data Movement: Does it primarily read inputs, write outputs, or transform data?
3. External Dependencies: Does it interact with external APIs, databases, or services?
4. Transformation vs Enrichment: Does it modify existing data or add new information?

Input Information:
- Name: {name}
- Documentation: {docstring}
- Code Excerpt: {code}
- Additional Hints: {hints}

Respond with JSON containing:
- component_type: One of the types listed above
- rationale: Brief explanation of why this classification was chosen
- confidence: Number between 0.0 and 1.0 indicating confidence in classification

Example Response:
{{
    "component_type": "Enricher",
    "rationale": "This component calls an external geocoding API to add location coordinates to existing address data",
    "confidence": 0.9
}}"""

REFINE_CONTRACT_PROMPT = """You are a data quality engineer. Your task is to review and improve a Great Expectations data contract to make it more robust and less prone to overfitting to initial sample data.

Your goal is to generalize the expectations to handle reasonable variations in real-world data while maintaining data quality standards.

Guidelines:
1. Enum Expansion: If value sets seem limited, suggest allowing additional reasonable values
2. Range Flexibility: Adjust numeric ranges to account for natural variation
3. Nullable Fields: Consider which fields might legitimately be null in some cases
4. String Lengths: Allow reasonable buffer for text fields
5. Data Types: Ensure type expectations are appropriate and flexible

Input:
- Current Contract: {suite_json}
- Sample Data: {samples_json}
- Additional Guidance: {guidance}

Please analyze the contract and sample data, then provide suggestions for improvement.

Respond with JSON containing:
- suggestions: List of specific suggestions with rationale
- updated_suite: Modified expectation suite with improvements applied
- changes_summary: Brief summary of what was changed and why
- risk_assessment: Assessment of any risks introduced by the changes

Example Response:
{{
    "suggestions": [
        {{
            "type": "enum_expansion",
            "field": "status",
            "current": ["active", "inactive"],
            "suggested": ["active", "inactive", "pending", "suspended"],
            "rationale": "Real-world systems often have transitional states"
        }}
    ],
    "updated_suite": {{
        "suite": "refined_contract_v1",
        "expectations": [...]
    }},
    "changes_summary": "Expanded status enum, increased string length buffers by 20%, made optional_field nullable",
    "risk_assessment": "Low risk - changes maintain data quality while reducing false positives"
}}"""

SCHEMA_GENERALIZATION_PROMPT = """You are a data schema expert. Your task is to generalize a data schema based on multiple samples to create a robust schema that can handle variations in real-world data.

Consider:
1. Field Optionality: Which fields are always present vs sometimes missing?
2. Data Types: What are the most appropriate and flexible types?
3. Value Ranges: What ranges accommodate the data variation?
4. Pattern Recognition: Are there patterns in string fields that should be preserved?

Input:
- Current Schema: {schema_json}
- Sample Records: {samples_json}
- Domain Context: {context}

Respond with JSON containing:
- generalized_schema: Updated schema with improvements
- changes: List of changes made with explanations
- flexibility_score: Number 0-10 indicating how flexible the new schema is
- quality_score: Number 0-10 indicating how well it maintains data quality

Example Response:
{{
    "generalized_schema": {{
        "fields": [
            {{"name": "id", "type": "string", "nullable": false}},
            {{"name": "status", "type": "string", "nullable": true, "enum": ["active", "inactive", "pending"]}}
        ]
    }},
    "changes": [
        {{"field": "status", "change": "made nullable", "reason": "10% of samples had missing status"}},
        {{"field": "count", "change": "expanded range", "reason": "samples showed values from 0-1000, expanded to 0-10000"}}
    ],
    "flexibility_score": 7,
    "quality_score": 8
}}"""

AUTO_TAG_PROMPT = """You are a data pipeline analyst. Your task is to suggest relevant tags for a data pipeline step based on its characteristics.

Consider these tag categories:
- data-source: Where data comes from (api, file, database, stream)
- operation: Type of operation (transform, enrich, validate, export)
- domain: Business domain (finance, marketing, operations, etc.)
- technology: Technologies used (pandas, spark, sql, rest-api)
- performance: Performance characteristics (batch, streaming, real-time)
- quality: Data quality aspects (validation, cleansing, profiling)

Input Information:
- Step Name: {name}
- Component Type: {component_type}
- Description: {description}
- Code Context: {code}

Suggest 3-7 relevant tags that would help with pipeline organization and discovery.

Respond with JSON containing:
- tags: Dictionary of tag category to tag value
- rationale: Brief explanation for tag choices
- additional_metadata: Any other useful metadata discovered

Example Response:
{{
    "tags": {{
        "data-source": "api",
        "operation": "enrich",
        "domain": "geospatial",
        "technology": "rest-api",
        "performance": "batch"
    }},
    "rationale": "Component enriches data via REST API calls, processes in batches, works with location data",
    "additional_metadata": {{
        "estimated_cost": "medium",
        "external_dependency": true,
        "parallelizable": true
    }}
}}"""