"""Module containing the Coordinator class to manage data requests from the Philips API."""

import asyncio
import contextlib
import json
import logging
from asyncio.tasks import Task
from collections.abc import AsyncIterator, Callable
from typing import Any

from aioairctrl import CoAPClient
from aiocoap import NON, Message
from aiocoap.numbers.codes import GET
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryNotReady

from .timer import Timer

_LOGGER = logging.getLogger(__name__)

MISSED_PACKAGE_COUNT = 3

# Re-registration delay after the observation itself ended; doubles on repeats, capped.
OBSERVE_RETRY_S = 2
OBSERVE_RETRY_MAX_S = 60


class Coordinator:
    """Class to coordinate the data requests from the Philips API."""

    def __init__(self, hass: HomeAssistant, client: CoAPClient, host: str, status: dict[str, Any] | None) -> None:
        """Initialize the Coordinator.

        :param hass: HomeAssistant instance.
        :param client: CoAPClient instance.
        :param host: Host address of the device.
        :param status: Initial status of the device.
        """

        self.hass = hass
        self.client = client
        self.host = host
        self.status = status

        self._listeners: list[CALLBACK_TYPE] = []
        self._task: Task | None = None

        self._reconnect_task: Task | None = None
        self._timeout: int = 60
        self._skipped_messages = 0

        self._timer_disconnected = Timer(
            timeout=self._timeout * MISSED_PACKAGE_COUNT,
            callback=self.reconnect,
            autostart=True,
        )
        self._timer_disconnected.setAutoRestart(True)

    async def shutdown(self):
        """Shutdown the API connection."""

        if self._reconnect_task is not None:
            self._reconnect_task.cancel()

        if self._timer_disconnected is not None:
            self._timer_disconnected.cancel()

        if self.client is not None:
            await self.client.shutdown()

    async def reconnect(self):
        """Reconnect to the API connection."""

        try:
            if self._reconnect_task is not None:
                self._reconnect_task.cancel()
                self._reconnect_task = None

            self._reconnect_task = asyncio.create_task(self._reconnect())

        except Exception:
            _LOGGER.exception("Exception on starting reconnect!")

    async def _reconnect(self):
        """Reconnect to the API connection."""

        try:
            # Stop observing first, or the observe loop keeps re-registering on the closed client.
            if self._task:
                self._task.cancel()
                self._task = None
            with contextlib.suppress(Exception):
                await self.client.shutdown()
            self.client = await CoAPClient.create(self.host)
            self._start_observing()

        except asyncio.CancelledError:
            # Silently drop this exception, because we are responsible for it.
            # Reconnect took to long
            pass

        except Exception:
            _LOGGER.exception("_reconnect error")

    async def async_first_refresh(self) -> None:
        """Refresh the data for the first time."""

        try:
            self.status, timeout = await self.client.get_status()
            self._timeout = timeout
            if self._timer_disconnected is not None:
                self._timer_disconnected.setTimeout(timeout * MISSED_PACKAGE_COUNT)
            _LOGGER.debug("finished first refresh for host %s", self.host)

        except Exception as ex:
            _LOGGER.exception("Config not ready, first refresh failed for host %s", self.host)
            raise ConfigEntryNotReady from ex

    @callback
    def async_add_listener(self, update_callback: CALLBACK_TYPE) -> Callable[[], None]:
        """Listen for data updates."""

        start_observing = not self._listeners
        self._listeners.append(update_callback)

        if start_observing:
            self._start_observing()

        @callback
        def remove_listener() -> None:
            """Remove update listener."""
            self.async_remove_listener(update_callback)

        return remove_listener

    @callback
    def async_remove_listener(self, update_callback) -> None:
        """Remove data update."""

        self._listeners.remove(update_callback)

        if not self._listeners and self._task:
            self._task.cancel()
            self._task = None

    def _decode(self, client: CoAPClient, response: Message) -> dict[str, Any] | None:
        """Return the reported state in a status message, or None if the message cannot be read."""

        try:
            payload = client._enc.decrypt(response.payload.decode())
            status = json.loads(payload)["state"]["reported"]
        except Exception as ex:  # one bad message must not end the observation
            self._skipped_messages += 1
            _LOGGER.warning(
                "Skipping undecodable status message from %s (%s: %s); %d skipped so far",
                self.host,
                type(ex).__name__,
                ex,
                self._skipped_messages,
            )
            _LOGGER.debug("undecodable payload: %r", response.payload)
            return None
        # Same text as aioairctrl's own log line, so log analysis counts pushes the same way.
        _LOGGER.debug("observation status: %s", payload)
        return status

    async def _observe_status(self) -> AsyncIterator[dict[str, Any]]:
        """Yield each status the device pushes, skipping messages that cannot be decoded.

        This is aioairctrl's Client.observe_status() with one difference. There, a message that cannot be
        decoded raises, and the whole observation ends with it. Some devices send such messages: seen on an
        HU5710 (firmware 0.4.7 / AWS_Philips_AIR_Combo@86), where about 4 % of the periodic reports carried NUL
        bytes inside the hex payload (ValueError from bytes.fromhex; the digest matched the corrupted text).
        The observation then stayed dead until the disconnect watchdog reconnected (max_age x
        MISSED_PACKAGE_COUNT, 180 s). Here the message is logged and skipped, and later pushes still arrive.
        The first answer is guarded too, and so is a well-formed message that lacks state.reported.

        Uses aioairctrl's client context and encryption context (_ctx, _enc), as of the pinned version.
        """

        client = self.client
        request = Message(code=GET, mtype=NON, uri=f"coap://{client.host}:{client.port}{client.STATUS_PATH}")
        request.opt.observe = 0
        requester = client._ctx.request(request)
        observation = requester.observation
        try:
            status = self._decode(client, await requester.response)
            if status is not None:
                yield status
            if observation is not None:
                async for response in observation:
                    status = self._decode(client, response)
                    if status is not None:
                        yield status
        finally:
            # An observation that ended with an error is already cancelled; cancelling again raises
            # AssertionError, which would hide the error (or a CancelledError) on its way out.
            if observation is not None and not observation.cancelled:
                observation.cancel()

    async def _async_observe_status(self) -> None:
        """Observe the status of the device.

        If the observation itself ends (an error from the CoAP layer, or the device ending it), register a new
        one after a short delay, rather than waiting for the disconnect watchdog.
        """

        delay = OBSERVE_RETRY_S
        while True:
            try:
                async for status in self._observe_status():
                    delay = OBSERVE_RETRY_S
                    self.status = status
                    self._timer_disconnected.reset()
                    for update_callback in self._listeners:
                        update_callback()
                _LOGGER.warning("Observation of %s ended; re-registering in %ss", self.host, delay)
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                _LOGGER.warning(
                    "Observation of %s failed (%s: %s); re-registering in %ss",
                    self.host,
                    type(ex).__name__,
                    ex,
                    delay,
                )
            await asyncio.sleep(delay)
            delay = min(delay * 2, OBSERVE_RETRY_MAX_S)

    def _start_observing(self) -> None:
        """Schedule state observation."""

        if self._task:
            self._task.cancel()
            self._task = None

        self._task = asyncio.create_task(self._async_observe_status())
        self._timer_disconnected.reset()
