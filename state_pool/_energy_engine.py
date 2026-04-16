# -*- coding: utf-8 -*-
"""StatePool energy update engine."""

from __future__ import annotations

import time

from ._id_generator import next_id


class EnergyEngine:
    """Apply energy deltas and maintain activation-side runtime modulation."""

    def __init__(self, config: dict):
        self._config = config

    def apply_energy_delta(
        self,
        item: dict,
        delta_er: float,
        delta_ev: float,
        tick_number: int,
        reason: str = "stimulus_apply",
        source_module: str = "",
        trace_id: str = "",
        tick_id: str = "",
        *,
        mark_active: bool = True,
    ) -> dict:
        energy = item["energy"]
        dynamics = item["dynamics"]
        now_ms = int(time.time() * 1000)

        before_er = float(energy["er"])
        before_ev = float(energy["ev"])
        before_cp_delta = float(energy["cognitive_pressure_delta"])
        before_cp_abs = float(energy["cognitive_pressure_abs"])

        new_er = before_er + float(delta_er)
        new_ev = before_ev + float(delta_ev)

        if self._config.get("energy_update_floor_to_zero", True) and not self._config.get("allow_negative_energy", False):
            new_er = max(0.0, new_er)
            new_ev = max(0.0, new_ev)

        new_er = round(new_er, 8)
        new_ev = round(new_ev, 8)
        new_cp_delta = round(new_er - new_ev, 8)
        new_cp_abs = round(abs(new_cp_delta), 8)

        energy["er"] = new_er
        energy["ev"] = new_ev
        energy["cognitive_pressure_delta"] = new_cp_delta
        energy["cognitive_pressure_abs"] = new_cp_abs
        energy["salience_score"] = round(max(new_er, new_ev), 8)

        actual_delta_er = round(new_er - before_er, 8)
        actual_delta_ev = round(new_ev - before_ev, 8)
        delta_cp_delta_val = round(new_cp_delta - before_cp_delta, 8)
        delta_cp_abs_val = round(new_cp_abs - before_cp_abs, 8)

        prev_update_ms = int(dynamics.get("last_update_at", now_ms))
        dt_ms = max(now_ms - prev_update_ms, int(self._config.get("tick_time_floor_ms", 1)))
        dt_s = dt_ms / 1000.0 if dt_ms > 0 else 0.001

        if self._config.get("enable_change_rate_tracking", True):
            er_rate = actual_delta_er / dt_s
            ev_rate = actual_delta_ev / dt_s
            cp_delta_rate = delta_cp_delta_val / dt_s
            cp_abs_rate = delta_cp_abs_val / dt_s
        else:
            er_rate = 0.0
            ev_rate = 0.0
            cp_delta_rate = 0.0
            cp_abs_rate = 0.0

        dynamics["prev_er"] = before_er
        dynamics["prev_ev"] = before_ev
        dynamics["delta_er"] = actual_delta_er
        dynamics["delta_ev"] = actual_delta_ev
        dynamics["er_change_rate"] = round(er_rate, 6)
        dynamics["ev_change_rate"] = round(ev_rate, 6)
        dynamics["prev_cp_delta"] = before_cp_delta
        dynamics["prev_cp_abs"] = before_cp_abs
        dynamics["delta_cp_delta"] = delta_cp_delta_val
        dynamics["delta_cp_abs"] = delta_cp_abs_val
        dynamics["cp_delta_rate"] = round(cp_delta_rate, 6)
        dynamics["cp_abs_rate"] = round(cp_abs_rate, 6)
        dynamics["last_update_tick"] = tick_number
        dynamics["last_update_at"] = now_ms
        dynamics["update_count"] = int(dynamics.get("update_count", 0)) + 1

        if mark_active:
            self._register_activation(item=item, tick_number=tick_number, now_ms=now_ms)

        item["updated_at"] = now_ms

        return self._build_change_event(
            target_item_id=item["id"],
            event_type="energy_update",
            trace_id=trace_id or item.get("trace_id", ""),
            tick_id=tick_id or item.get("tick_id", ""),
            before_er=before_er,
            before_ev=before_ev,
            before_cp_delta=before_cp_delta,
            before_cp_abs=before_cp_abs,
            after_er=new_er,
            after_ev=new_ev,
            after_cp_delta=new_cp_delta,
            after_cp_abs=new_cp_abs,
            delta_er=actual_delta_er,
            delta_ev=actual_delta_ev,
            delta_cp_delta=delta_cp_delta_val,
            delta_cp_abs=delta_cp_abs_val,
            er_rate=er_rate,
            ev_rate=ev_rate,
            cp_delta_rate=cp_delta_rate,
            cp_abs_rate=cp_abs_rate,
            reason=reason,
            source_module=source_module,
        )

    def apply_decay(
        self,
        item: dict,
        er_ratio: float,
        ev_ratio: float,
        tick_number: int,
        trace_id: str = "",
        tick_id: str = "",
    ) -> dict:
        energy = item["energy"]
        current_er = float(energy["er"])
        current_ev = float(energy["ev"])
        new_er = current_er * float(er_ratio)
        new_ev = current_ev * float(ev_ratio)

        event = self.apply_energy_delta(
            item=item,
            delta_er=new_er - current_er,
            delta_ev=new_ev - current_ev,
            tick_number=tick_number,
            reason="tick_decay",
            source_module="state_pool",
            trace_id=trace_id,
            tick_id=tick_id,
            mark_active=False,
        )
        event["event_type"] = "decay"
        energy["last_decay_tick"] = tick_number
        energy["last_decay_at"] = int(time.time() * 1000)
        return event

    def seed_runtime_modulation(self, item: dict, tick_number: int) -> None:
        self._register_activation(item=item, tick_number=tick_number, now_ms=int(time.time() * 1000))

    def recalc_cognitive_pressure(self, item: dict):
        energy = item["energy"]
        er = float(energy.get("er", 0.0))
        ev = float(energy.get("ev", 0.0))
        energy["cognitive_pressure_delta"] = round(er - ev, 8)
        energy["cognitive_pressure_abs"] = round(abs(er - ev), 8)

    def _register_activation(self, *, item: dict, tick_number: int, now_ms: int) -> None:
        energy = item.setdefault("energy", {})
        lifecycle = item.setdefault("lifecycle", {})

        history = self._trim_recent_activation_ticks(lifecycle.get("recent_activation_ticks", []), tick_number)
        if not history or int(history[-1]) != int(tick_number):
            history.append(int(tick_number))

        lifecycle["recent_activation_ticks"] = history
        lifecycle["last_active_tick"] = int(tick_number)
        lifecycle["last_recency_refresh_tick"] = int(tick_number)
        lifecycle["recency_hold_ticks_remaining"] = self._recency_hold_ticks()

        energy["recency_gain"] = self._recency_peak()
        energy["fatigue"] = self._fatigue_from_count(len(history))
        energy.setdefault("last_decay_tick", 0)
        energy.setdefault("last_decay_at", now_ms)
        item["updated_at"] = now_ms

    def _trim_recent_activation_ticks(self, history: list[int] | tuple[int, ...], current_tick: int) -> list[int]:
        window = max(1, int(self._config.get("fatigue_window_ticks", 12)))
        min_tick = int(current_tick) - window + 1
        return [
            int(tick)
            for tick in list(history or [])
            if isinstance(tick, int) or str(tick).isdigit()
            if int(tick) >= min_tick
        ]

    def _fatigue_from_count(self, count: int) -> float:
        threshold = max(1, int(self._config.get("fatigue_threshold_count", 3)))
        window = max(threshold, int(self._config.get("fatigue_window_ticks", 12)))
        max_value = max(0.0, min(1.0, float(self._config.get("fatigue_max_value", 1.0))))
        if int(count) < threshold:
            return 0.0
        numerator = int(count) - threshold + 1
        denominator = max(1, window - threshold + 1)
        return round(max_value * min(1.0, float(numerator) / float(denominator)), 8)

    def _recency_peak(self) -> float:
        return round(max(1.0, float(self._config.get("recency_gain_peak", 10.0))), 8)

    def _recency_hold_ticks(self) -> int:
        return max(0, int(self._config.get("recency_gain_hold_ticks", 2)))

    @staticmethod
    def _build_change_event(
        target_item_id: str,
        event_type: str,
        trace_id: str,
        tick_id: str,
        before_er: float,
        before_ev: float,
        before_cp_delta: float,
        before_cp_abs: float,
        after_er: float,
        after_ev: float,
        after_cp_delta: float,
        after_cp_abs: float,
        delta_er: float,
        delta_ev: float,
        delta_cp_delta: float,
        delta_cp_abs: float,
        er_rate: float,
        ev_rate: float,
        cp_delta_rate: float,
        cp_abs_rate: float,
        reason: str,
        source_module: str,
    ) -> dict:
        return {
            "event_id": next_id("sce"),
            "event_type": event_type,
            "target_item_id": target_item_id,
            "trace_id": trace_id,
            "tick_id": tick_id,
            "timestamp_ms": int(time.time() * 1000),
            "before": {
                "er": round(before_er, 8),
                "ev": round(before_ev, 8),
                "cp_delta": round(before_cp_delta, 8),
                "cp_abs": round(before_cp_abs, 8),
            },
            "after": {
                "er": round(after_er, 8),
                "ev": round(after_ev, 8),
                "cp_delta": round(after_cp_delta, 8),
                "cp_abs": round(after_cp_abs, 8),
            },
            "delta": {
                "delta_er": round(delta_er, 8),
                "delta_ev": round(delta_ev, 8),
                "delta_cp_delta": round(delta_cp_delta, 8),
                "delta_cp_abs": round(delta_cp_abs, 8),
            },
            "rate": {
                "er_change_rate": round(er_rate, 6),
                "ev_change_rate": round(ev_rate, 6),
                "cp_delta_rate": round(cp_delta_rate, 6),
                "cp_abs_rate": round(cp_abs_rate, 6),
            },
            "reason": reason,
            "source_module": source_module,
            "extra_context": {},
        }

    def update_config(self, config: dict):
        self._config = config
