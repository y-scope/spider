{{/*
Expands the name of the chart.

@return {string} The chart name (truncated to 63 characters)
*/}}
{{- define "spider.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Creates a default fully qualified app name (truncated to 63 chars for the DNS naming spec). If the
release name already contains the chart name it is used as-is.

@return {string} The fully qualified app name (truncated to 63 characters)
*/}}
{{- define "spider.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}{{/* if contains $name .Release.Name */}}
{{- end }}{{/* if .Values.fullnameOverride */}}
{{- end }}{{/* define "spider.fullname" */}}

{{/*
Creates a fully qualified component name while preserving its suffix within the 63-character limit.

@param {object} root Root template context (required)
@param {string} component Component name suffix (required)
@return {string} The fully qualified component name
*/}}
{{- define "spider.componentFullname" -}}
{{- $suffix := printf "-%s" .component -}}
{{- $maxBaseLength := sub 63 (len $suffix) | int -}}
{{- $base := include "spider.fullname" .root | trunc $maxBaseLength | trimSuffix "-" -}}
{{- printf "%s%s" $base $suffix -}}
{{- end }}

{{/*
Creates chart name and version as used by the chart label.

@return {string} Chart name and version (truncated to 63 characters)
*/}}
{{- define "spider.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Creates common labels for all resources.

@return {string} YAML-formatted common labels
*/}}
{{- define "spider.labels" -}}
helm.sh/chart: {{ include "spider.chart" . }}
{{ include "spider.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Creates selector labels for matching resources.

@return {string} YAML-formatted selector labels
*/}}
{{- define "spider.selectorLabels" -}}
app.kubernetes.io/name: {{ include "spider.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Creates a container image reference from .Values.image.

Renders repository@digest when "digest" is set; otherwise, requires "tag" and renders repository:tag.

@param {object} root Root template context (required)
@param {string} component Key under .Values.image (e.g., "storage", "database")
@return {string} Full image reference (repository@digest or repository:tag)
*/}}
{{- define "spider.imageRef" -}}
{{- $img := index .root.Values.image .component -}}
{{- if $img.digest -}}
{{- printf "%s@%s" $img.repository $img.digest -}}
{{- else -}}
{{- $tag := required (printf "image.%s.tag is required" .component) $img.tag -}}
{{- printf "%s:%s" $img.repository $tag -}}
{{- end -}}
{{- end }}

{{/*
Creates timings for readiness probes (faster checks for quicker startup).

@return {string} YAML-formatted readiness probe timing configuration
*/}}
{{- define "spider.readinessProbeTimings" -}}
initialDelaySeconds: 6
periodSeconds: 2
timeoutSeconds: 2
failureThreshold: 10
{{- end }}

{{/*
Creates timings for liveness probes.

@return {string} YAML-formatted liveness probe timing configuration
*/}}
{{- define "spider.livenessProbeTimings" -}}
initialDelaySeconds: 180
periodSeconds: 30
timeoutSeconds: 4
failureThreshold: 3
{{- end }}

{{/*
Gets the bundled database Service host or external `spiderConfig.database.host`.

@param {object} . Root template context
@return {string} The database host
*/}}
{{- define "spider.databaseHost" -}}
{{- if has "database" .Values.spiderConfig.bundled -}}
{{- include "spider.componentFullname" (dict "root" . "component" "database") -}}
{{- else -}}
{{- .Values.spiderConfig.database.host -}}
{{- end -}}
{{- end }}

{{/*
Gets the bundled database port or external `spiderConfig.database.port`.

@param {object} . Root template context
@return {string} The database port
*/}}
{{- define "spider.databasePort" -}}
{{- if has "database" .Values.spiderConfig.bundled -}}
3306
{{- else -}}
{{- .Values.spiderConfig.database.port -}}
{{- end -}}
{{- end }}
