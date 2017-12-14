import os
import json
import logging
from autobahn.asyncio.wamp import ApplicationRunner, ApplicationSession
import trollius as asyncio
from trollius import From

log = logging.getLogger(__name__)

class WampClientComponent(ApplicationSession):
    """Connector to join and execute a WAMP session."""

    _data_uri = ''
    _results = None
    _timeout = 20.

    def __init__(self, results, data_uri, *args, **kwargs):
        super(WampClientComponent, self).__init__(*args, **kwargs)
        self._data_uri = data_uri
        self._results = results
        log.debug(u'Creating Lintol WAMP client')

    @asyncio.coroutine
    def onJoin(self, details):
        """When we join the server, execute the client workflow."""

        log.debug(u'Beginning validation')

        try:
            fut = self.call(
                u'com.ltlcapstone.validation',
                self._data_uri
            )

            try:
                validation_id = yield From(asyncio.wait_for(fut, self._timeout))
            except asyncio.TimeoutError as e:
                fut.cancel()
                log.debug(u'Timed out waiting for Capstone')
                raise e

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
        except Exception as e:
            log.exception(u'Exception in the WAMP procedure')
            raise e
        finally:
            loop = asyncio.get_event_loop()
            loop.stop()


def launch_wamp(data_uri):
    """Run the workflow against a WAMP server."""

    log.debug(u'Executing Lintol for URI: {}'.format(data_uri))

    runner = ApplicationRunner(url=u'ws://172.18.0.1:8080/ws', realm=u'realm1')
    results = []

    # Outside the main thread, we must do this
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(
        runner.run(lambda *args, **kwargs: WampClientComponent(
            results,
            data_uri,
            *args,
            **kwargs
        ), start_loop=False)
    )

    loop.run_forever()

    log.debug(u'Finished Lintol event loop')

    loop.close()

    if results:
        output = json.loads(results[0][0])
        log.debug(output)
        goodtables_all = output[0]['goodtables:all'][2]

        return goodtables_all

    return False
