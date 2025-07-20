import logging

import pyrogram
from pyrogram.raw.types.update_new_message import UpdateNewMessage
from pyrogram.raw.functions.updates.get_difference import GetDifference
from pyrogram.raw.types.updates.difference import Difference
from pyrogram.raw.types.updates.difference_empty import DifferenceEmpty
from pyrogram.raw.types.updates.difference_slice import DifferenceSlice
from pyrogram.raw.types.updates.difference_too_long import DifferenceTooLong
from pyrogram.raw.types.updates_too_long import UpdatesTooLong

log = logging.getLogger(__name__)

class Recover:
    async def recover_updates(self: "pyrogram.Client"):
        """
        Recovers missed updates (messages)
        """
        state = self.storage.get_state(0)

        local_pts = state.pts
        local_date = state.date
        local_seq = state.seq
        
        message_updates_counter = 0
        other_updates_counter = 0

        log.info("Started recovering...")

        prev_pts = 0

        while True:
            try:
                diff = await self.invoke(
                    GetDifference(
                        pts=local_pts,
                        date=local_date,
                        qts=0,
                        pts_total_limit=1000000,
                    )
                )

            except Exception as e:
                log.exception(e)
                break

            if isinstance(diff, DifferenceEmpty):
                self.storage.update_state(
                        id=state.id,
                        pts=local_pts,
                        date=diff.date,
                        seq=diff.seq
                )
                break

            if isinstance(diff, DifferenceTooLong):
                self.storage.update_state(
                        id=state.id,
                        pts=diff.pts,
                        date=local_date,
                        seq=local_seq
                )
                continue

            elif isinstance(diff, Difference):
                local_pts = diff.state.pts
                local_date = diff.state.date
                local_seq = diff.state.seq

            elif isinstance(diff, DifferenceSlice):
                local_pts = diff.intermediate_state.pts
                local_date = diff.intermediate_state.date
                local_seq = diff.intermediate_state.seq

                if prev_pts == local_pts:
                    break

                prev_pts = local_pts

            
            users = {u.id: u for u in diff.users}  # type: ignore
            chats = {c.id: c for c in diff.chats}  # type: ignore

            for message in diff.new_messages:
                message_updates_counter += 1
                self.dispatcher.updates_queue.put_nowait(
                    (
                        UpdateNewMessage(
                            message=message,
                            pts=local_pts,
                            pts_count=-1
                        ),
                        users,
                        chats
                    )
                )

        log.info("Recovered %s messages and %s updates", message_updates_counter, other_updates_counter)
