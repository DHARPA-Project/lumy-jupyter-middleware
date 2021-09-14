import logging

from lumy_middleware.notes.inmemory import InMemoryNotesStore as NotesStore
from lumy_middleware.jupyter.base import MessageHandler
from lumy_middleware.types.generated import (MsgNotesAdd, MsgNotesDelete,
                                             MsgNotesGetNotes, MsgNotesNotes,
                                             MsgNotesUpdate)

logger = logging.getLogger(__name__)


class NotesHandler(MessageHandler):
    def _handle_GetNotes(self, msg: MsgNotesGetNotes):
        notes = NotesStore.get_instance().get_notes(msg.step_id)
        return MsgNotesNotes(step_id=msg.step_id, notes=notes)

    def _handle_Add(self, msg: MsgNotesAdd):
        NotesStore.get_instance().add_note(msg.step_id, msg.note)
        notes = NotesStore.get_instance().get_notes(msg.step_id)
        return MsgNotesNotes(step_id=msg.step_id, notes=notes)

    def _handle_Update(self, msg: MsgNotesUpdate):
        NotesStore.get_instance().update_note(msg.step_id, msg.note)
        notes = NotesStore.get_instance().get_notes(msg.step_id)
        return MsgNotesNotes(step_id=msg.step_id, notes=notes)

    def _handle_Delete(self, msg: MsgNotesDelete):
        NotesStore.get_instance().delete_note(msg.step_id, msg.note_id)
        notes = NotesStore.get_instance().get_notes(msg.step_id)
        return MsgNotesNotes(step_id=msg.step_id, notes=notes)
