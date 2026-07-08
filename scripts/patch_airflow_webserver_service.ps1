param(
  [string]$Namespace = "airflow",
  [string]$ServiceName = "airflow-webserver",
  [ValidateSet("Local", "Cluster")]
  [string]$ExternalTrafficPolicy = "Local"
)

$ErrorActionPreference = "Stop"

$patch = @{
  spec = @{
    externalTrafficPolicy = $ExternalTrafficPolicy
  }
} | ConvertTo-Json -Compress -Depth 4

$patchFile = Join-Path $env:TEMP "airflow-webserver-service-patch.json"

try {
  Set-Content -LiteralPath $patchFile -Value $patch -Encoding ascii -NoNewline

  kubectl patch service $ServiceName `
    --namespace $Namespace `
    --type merge `
    --patch-file $patchFile
}
finally {
  Remove-Item -LiteralPath $patchFile -Force -ErrorAction SilentlyContinue
}

kubectl get service $ServiceName `
  --namespace $Namespace `
  -o "jsonpath={.metadata.name}{' type='}{.spec.type}{' externalTrafficPolicy='}{.spec.externalTrafficPolicy}{' ingressIP='}{.status.loadBalancer.ingress[0].ip}{'\n'}"
