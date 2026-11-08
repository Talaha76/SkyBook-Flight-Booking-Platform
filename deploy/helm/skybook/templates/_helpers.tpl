{{- define "skybook.labels" -}}
app.kubernetes.io/part-of: skybook
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}

{{- define "skybook.secretName" -}}
{{- if .Values.secrets.existingSecret }}{{ .Values.secrets.existingSecret }}{{ else }}skybook-secrets{{ end }}
{{- end }}

{{- define "skybook.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag }}
{{- end }}

{{/* Environment shared by every SkyBook container */}}
{{- define "skybook.commonEnv" -}}
- name: KAFKA_BROKERS
  value: {{ .Values.connections.kafkaBrokers | quote }}
- name: REDIS_URL
  value: {{ .Values.connections.redisUrl | quote }}
- name: LOG_LEVEL
  value: {{ .Values.logLevel | quote }}
- name: INTERNAL_TOKEN
  valueFrom: { secretKeyRef: { name: {{ include "skybook.secretName" . }}, key: internalToken } }
- name: PGPASSWORD
  valueFrom: { secretKeyRef: { name: {{ include "skybook.secretName" . }}, key: postgresPassword } }
{{- end }}
