param(
    [string]$Namespace = "airflow",
    [string]$Release = "airflow",
    [int]$Tail = 120
)

$ErrorActionPreference = "Continue"

function Write-Section {
    param([string]$Title)
    Write-Output ""
    Write-Output "===== $Title ====="
}

Write-Section "Context"
kubectl config current-context
helm status $Release -n $Namespace

Write-Section "Workloads"
kubectl get pods,jobs,pvc,svc -n $Namespace -o wide

Write-Section "Recent Events"
kubectl get events -n $Namespace --sort-by=.lastTimestamp | Select-Object -Last 100

Write-Section "Node Capacity"
kubectl get nodes -o wide
kubectl top nodes
kubectl top pods -n $Namespace

$pods = kubectl get pods -n $Namespace -o jsonpath="{range .items[*]}{.metadata.name}{'\n'}{end}"
foreach ($pod in $pods) {
    if (-not $pod) {
        continue
    }

    Write-Section "Describe Pod: $pod"
    kubectl describe pod $pod -n $Namespace

    $containers = kubectl get pod $pod -n $Namespace -o jsonpath="{range .spec.initContainers[*]}{.name}{'\n'}{end}{range .spec.containers[*]}{.name}{'\n'}{end}"
    foreach ($container in $containers) {
        if (-not $container) {
            continue
        }

        Write-Section "Logs: $pod / $container"
        kubectl logs $pod -n $Namespace -c $container --tail=$Tail

        Write-Section "Previous Logs: $pod / $container"
        kubectl logs $pod -n $Namespace -c $container --previous --tail=$Tail
    }
}

exit 0
