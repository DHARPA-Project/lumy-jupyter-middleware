
from collections import defaultdict
from typing import Dict, List
from uuid import uuid4
from lumy_middleware.types.generated import Note
from datetime import datetime


def generate_id() -> str:
    return str(uuid4())


class InMemoryNotesStore:
    __instance = None
    notes: Dict[str, List[Note]] = defaultdict(list)

    @staticmethod
    def get_instance():
        if InMemoryNotesStore.__instance is None:
            InMemoryNotesStore.__instance = InMemoryNotesStore()

        return InMemoryNotesStore.__instance

    def get_notes(self, step_id: str) -> List[Note]:
        return self.notes[step_id]

    def add_note(self, step_id: str, note: Note):
        notes = self.notes[step_id]
        notes.append(Note(content=note.content, id=generate_id(),
                          created_at=datetime.now().isoformat()))
        self.notes[step_id] = notes

    def update_note(self, step_id: str, note: Note):
        notes = self.notes[step_id]
        for n in notes:
            if n.id == note.id:
                n.content = note.content

    def delete_note(self, step_id: str, note_id: str):
        notes = [n for n in self.notes[step_id] if n.id != note_id]
        self.notes[step_id] = notes
