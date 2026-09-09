"""Produit des tickets clients aléatoires dans le topic Redpanda client_tickets."""

import json
import os
import random
import time
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Producer


BROKERS = os.getenv("REDPANDA_BROKERS", "localhost:19092")
TOPIC = os.getenv("REDPANDA_TOPIC", "client_tickets")
INTERVAL_SECONDS = float(os.getenv("TICKET_INTERVAL_SECONDS", "1"))
MAX_MESSAGES = int(os.getenv("TICKET_MAX_MESSAGES", "20"))

REQUESTS = [
    ("Impossible de me connecter à mon compte", "technical"),
    ("Je souhaite modifier mon adresse de facturation", "billing"),
    ("Ma commande n'est pas encore arrivée", "delivery"),
    ("Je demande le remboursement de ma commande", "refund"),
    ("Je souhaite obtenir des informations sur un produit", "information"),
]

PRIORITIES = ["low", "medium", "high", "critical"]


def create_ticket() -> dict[str, str]:
    """Construit un ticket contenant tous les champs exigés par l'énoncé."""
    request, request_type = random.choice(REQUESTS)

    return {
        "ticket_id": f"TICKET-{uuid4().hex[:8].upper()}",
        "client_id": f"CLIENT-{random.randint(1, 9999):04d}",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "request": request,
        "request_type": request_type,
        "priority": random.choice(PRIORITIES),
    }


def delivery_report(error, message) -> None:
    """Affiche le résultat de la livraison d'un message à Redpanda."""
    if error is not None:
        print(f"Échec de livraison : {error}")
        return

    print(
        "Livré : "
        f"topic={message.topic()} "
        f"partition={message.partition()} "
        f"offset={message.offset()}"
    )


def main() -> None:
    producer = Producer(
        {
            "bootstrap.servers": BROKERS,
            "client.id": "indutechdata-ticket-producer",
        }
    )

    print(f"Connexion à {BROKERS} - topic {TOPIC}")

    try:
        for number in range(1, MAX_MESSAGES + 1):
            ticket = create_ticket()
            payload = json.dumps(ticket, ensure_ascii=False).encode("utf-8")

            producer.produce(
                topic=TOPIC,
                key=ticket["ticket_id"].encode("utf-8"),
                value=payload,
                callback=delivery_report,
            )
            producer.poll(0)

            print(f"Ticket {number}/{MAX_MESSAGES} envoyé : {ticket}")
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("Arrêt demandé par l'utilisateur.")
    finally:
        remaining = producer.flush(10)
        if remaining:
            raise RuntimeError(f"{remaining} message(s) non livré(s)")
        print("Tous les tickets ont été livrés.")


if __name__ == "__main__":
    main()

