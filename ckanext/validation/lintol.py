import os
import json
import logging
from autobahn.asyncio.wamp import ApplicationRunner, ApplicationSession
import trollius as asyncio
from trollius import From
from ckantoolkit import config

log = logging.getLogger(__name__)

class WampClientComponent(ApplicationSession):
    """Connector to join and execute a WAMP session."""

    _data_uri = ''
    _results = None
    _timeout = 120.

    def __init__(self, results, data_uri, settings, *args, **kwargs):
        super(WampClientComponent, self).__init__(*args, **kwargs)
        self._token = config.get('ckanext.validation.lintol_token', None)

        self._data_uri = data_uri
        self._results = results
        self._settings = settings
        log.debug(u'Creating Lintol WAMP client')

    @asyncio.coroutine
    def onJoin(self, details):
        """When we join the server, execute the client workflow."""

        log.debug(u'Beginning validation')

        try:
            fut = self.call(
                u'com.ltlcapstone.validation',
                self._token,
                self._data_uri,
                self._settings
            )

            try:
                validation_ids = yield From(asyncio.wait_for(fut, self._timeout))
            except asyncio.TimeoutError as e:
                fut.cancel()
                log.debug(u'Timed out waiting for Capstone')
                raise e

            if validation_ids:
                validation_id = validation_ids[0]
                log.debug(u'Got Lintol validation id: {}'.format(validation_id))

                # Replace with loop.create_future() when available
                fut = asyncio.Future()

                @asyncio.coroutine
                def receive_response_event(result):
                    log.debug('Received response')
                    fut.set_result(result)

                yield From(self.subscribe(
                    receive_response_event,
                    u'com.ltlcapstone.validation.{validation_id}.event_complete'.format(
                        validation_id=validation_id
                    )
                ))

                try:
                    log.debug('Waiting for com.ltlcapstone.validation.{validation_id}.event_complete'.format(
                        validation_id=validation_id
                    ))
                    result = yield From(asyncio.wait_for(fut, self._timeout))
                except asyncio.TimeoutError as e:
                    fut.cancel()
                    log.debug(u'Timed out waiting for validation result')
                    raise e
                else:
                    self._results.append(result)
                    log.debug(u'Got validation result: ' + str(result))
            else:
                log.debug(u'No validation was run')
        except Exception as e:
            log.exception(u'Exception in the WAMP procedure')
            raise e
        finally:
            loop = asyncio.get_event_loop()
            loop.stop()


def launch_wamp(data_uri, format, schema, options):
    """Run the workflow against a WAMP server."""

    log.debug(u'Executing Lintol for URI: {}'.format(data_uri))

    runner = ApplicationRunner(url=u'ws://172.18.0.1:8080/ws', realm=u'realm1')
    results = []

    # Outside the main thread, we must do this
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    settings = {'fileType': format, 'schema': schema, 'options': options}

    loop.run_until_complete(
        runner.run(lambda *args, **kwargs: WampClientComponent(
            results,
            data_uri,
            settings,
            *args,
            **kwargs
        ), start_loop=False)
    )

    loop.run_forever()

    log.debug(u'Finished Lintol event loop')

    loop.close()

    if results:
        output = json.loads(results[0])
        log.debug(output)

        return output

    return False
