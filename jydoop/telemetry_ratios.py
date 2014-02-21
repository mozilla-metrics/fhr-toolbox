import jydoop
import healthreportutils

channels = {
    'nightly': 'nightly',
    'aurora': 'aurora',
    'beta': 'beta',
    'release': '1pct'
}

channel_key = "org.mozilla.fhrtoolbox.channel"

def setupjob(job, args):
    channel, = args
    if not channel in channels:
        raise ValueError("Unexpected channel")

    job.getConfiguration().set(channel_key, channel)
    healthreportutils.setup_sequence_scan(job, [channels[channel]])

@healthreportutils.FHRMapper(max_day_age=90)
def map(key, payload, context):
    if payload.channel != context.getConfiguration().get(channel_key):
        return

    enabled = payload.telemetry_enabled
    if enabled is None:
        enabled = '?'

    context.write(enabled, 1)

combine = jydoop.sumreducer
reduce = jydoop.sumreducer

