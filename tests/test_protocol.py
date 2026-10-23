import unittest

from cangjie_supervisor.protocol import (
    PROTOCOL_VERSION,
    ControlAction,
    Envelope,
    LeaseRef,
    MessageType,
    ProtocolError,
    claim,
    complete,
    control,
    heartbeat,
    hello,
    lease_grant,
    progress,
)


class ProtocolTests(unittest.TestCase):
    def lease(self) -> LeaseRef:
        return LeaseRef(
            workflow_id="wf-001",
            job_id="job-001",
            lease_token="lease-token",
            lease_generation=3,
        )

    def test_envelope_round_trip(self) -> None:
        original = hello("ubuntu-worker-01", ["gpu", "ocr"])
        rebuilt = Envelope.from_dict(original.to_dict())
        self.assertEqual(rebuilt.protocol_version, PROTOCOL_VERSION)
        self.assertEqual(rebuilt.message_type, MessageType.HELLO)
        self.assertEqual(rebuilt.worker_id, "ubuntu-worker-01")
        self.assertEqual(rebuilt.body["capabilities"], ["gpu", "ocr"])
    def test_execution_messages_carry_full_lease_identity(self) -> None:
        lease = self.lease()
        messages = [
            heartbeat("ubuntu-worker-01", lease),
            progress("ubuntu-worker-01", lease, checkpoint="page-100"),
            complete("ubuntu-worker-01", lease, {"ok": True}),
        ]
        for message in messages:
            ref = LeaseRef.from_dict(message.body["lease"])
            self.assertEqual(ref.job_id, "job-001")
            self.assertEqual(ref.lease_generation, 3)
            self.assertEqual(ref.lease_token, "lease-token")

    def test_claim_and_lease_grant_round_trip(self) -> None:
        request = claim("ubuntu-worker-01", "job-001")
        self.assertEqual(request.message_type, MessageType.CLAIM)
        self.assertEqual(request.body["job_id"], "job-001")
        response = lease_grant(
            "ubuntu-worker-01",
            self.lease(),
            "2026-09-21T10:00:00+00:00",
            idempotent=True,
        )
        rebuilt = Envelope.from_dict(response.to_dict())
        self.assertEqual(rebuilt.message_type, MessageType.LEASE_GRANT)
        ref = LeaseRef.from_dict(rebuilt.body["lease"])
        self.assertEqual(ref.lease_generation, 3)
        self.assertTrue(rebuilt.body["idempotent"])

    def test_control_can_target_current_lease(self) -> None:
        message = control(
            "ubuntu-worker-01",
            ControlAction.PAUSE,
            self.lease(),
        )
        self.assertEqual(message.body["action"], "PAUSE")
        ref = LeaseRef.from_dict(message.body["lease"])
        self.assertEqual(ref.workflow_id, "wf-001")

    def test_invalid_protocol_version_rejected(self) -> None:
        data = hello("ubuntu-worker-01", []).to_dict()
        data["protocol_version"] = "other/9"
        with self.assertRaises(ProtocolError):
            Envelope.from_dict(data)

    def test_invalid_lease_generation_rejected(self) -> None:
        with self.assertRaises(ProtocolError):
            LeaseRef("wf", "job", "token", 0).validate()


if __name__ == "__main__":
    unittest.main(verbosity=2)
