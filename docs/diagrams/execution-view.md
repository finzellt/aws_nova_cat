```mermaid
flowchart LR
  NC[NameCheckAndReconcile] --> IN[InitializeNova]
  NC -->|yields nova_id| NEW[IngestNewNova]

  NEW --> RR[RefreshReferences]
  NEW --> DSP[DiscoverSpectraProducts]

  RR -->|links reference_id| RR2["ComputeDiscoveryDate</br>(inside RefreshReferences)"]

  DSP -->|data_product_id discovered| AAV[AcquireAndValidateSpectra]

  UP[Upload/Registration] --> IPD[IngestPhotometry]

```
