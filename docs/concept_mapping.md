# Prefect and Datahub concept mapping


Prefect concepts are documented [here](https://docs.prefect.io/latest/concepts/), and datahub concepts are documented [here](https://datahubproject.io/docs/what-is-datahub/datahub-concepts).

Prefect Concept | DataHub Concept | URN | Possible Values
--- | --- | --- | ---
[Flow](https://docs.prefect.io/2.10.13/concepts/flows/#flows) | [DataFlow](https://datahubproject.io/docs/generated/metamodel/entities/dataflow/) | urn:li:dataFlow:(prefect, [platform-instance.]&lt;flow-name&gt;,prod) | &lt;flow-name&gt; is the user given a name like “etl”. if flow-name is not set by a user then prefect derive it from function-name annotated with @flow
[Flow Run](https://docs.prefect.io/latest/concepts/flows/#flow-runs) | [DataProcessInstance](https://datahubproject.io/docs/generated/metamodel/entities/dataprocessinstance) | urn:li:dataFlow:(prefect, [platform-instance.]&lt;flow-name&gt;,prod) | &lt;flow-name&gt; is the user given a name like “etl”. if flow-name is not set by a user then prefect derive it from function-name annotated with @flow
[Flow](https://docs.prefect.io/2.10.13/concepts/flows/#flows) | [DataFlow](https://datahubproject.io/docs/generated/metamodel/entities/dataflow/) | urn:li:dataFlow:(prefect, [platform-instance.]&lt;flow-name&gt;,prod) | &lt;flow-name&gt; is the user given a name like “etl”. if flow-name is not set by a user then prefect derive it from function-name annotated with @flow
