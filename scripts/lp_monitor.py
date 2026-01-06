"""
lp_monitor.py

CLMM LP position monitor that tracks positions and triggers actions based on price changes.

BEHAVIOR
--------
- Fetches all positions owned by the wallet on startup
- Monitors price changes from the initial price when monitoring started
- Triggers alert or auto-close when price change exceeds threshold

PARAMETERS
----------
- connector: CLMM connector in format 'name/type' (e.g. raydium/clmm, meteora/clmm)
- price_change_trigger_pct: Price change percentage to trigger action (default: 20%)
- trigger_action: Action to take when trigger is reached ('alert' or 'close')

TRIGGERS (extensible)
---------------------
- Price Change %: Triggers when price moves by specified % from initial price

ACTIONS
-------
- alert: Send notification only (default)
- close: Send notification and automatically close position
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Optional

from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.gateway.common_types import ConnectorType, get_connector_type
from hummingbot.connector.gateway.gateway_lp import CLMMPoolInfo, CLMMPositionInfo
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


@dataclass
class MonitoredPosition:
    """Tracks a position being monitored"""
    position_info: CLMMPositionInfo
    pool_info: Optional[CLMMPoolInfo]
    initial_price: Decimal
    initial_time: float
    triggered: bool = False
    pending_close_order_id: Optional[str] = None


class LpMonitorConfig(BaseClientModel):
    script_file_name: str = os.path.basename(__file__)
    connector: str = Field("raydium/clmm", json_schema_extra={
        "prompt": "CLMM connector in format 'name/type' (e.g. raydium/clmm, meteora/clmm)", "prompt_on_new": True})
    price_change_trigger_pct: Decimal = Field(Decimal("20"), json_schema_extra={
        "prompt": "Price change % to trigger action (e.g. 20 for 20%)", "prompt_on_new": True})
    trigger_action: str = Field("alert", json_schema_extra={
        "prompt": "Action when triggered: 'alert' (notify only) or 'close' (auto-close position)", "prompt_on_new": True})


class LpMonitor(ScriptStrategyBase):
    """
    CLMM LP position monitor that tracks positions and triggers actions based on price changes.
    """

    @classmethod
    def init_markets(cls, config: LpMonitorConfig):
        # We need at least one trading pair to initialize, will discover actual pairs from positions
        cls.markets = {config.connector: set()}

    def __init__(self, connectors: Dict[str, ConnectorBase], config: LpMonitorConfig):
        super().__init__(connectors)
        self.config = config
        self.exchange = config.connector
        self.connector_type = get_connector_type(config.connector)

        # Verify this is a CLMM connector
        if self.connector_type != ConnectorType.CLMM:
            raise ValueError(f"This script only supports CLMM connectors. Got: {config.connector}")

        # Validate trigger action
        if config.trigger_action not in ("alert", "close"):
            raise ValueError(f"Invalid trigger_action: {config.trigger_action}. Must be 'alert' or 'close'")

        # State tracking
        self.monitored_positions: Dict[str, MonitoredPosition] = {}  # position_address -> MonitoredPosition
        self.initialized: bool = False
        self.last_update_time: float = 0
        self.update_interval: float = 5.0  # Update every 5 seconds

        # Log startup information
        self.log_with_clock(logging.INFO,
                            f"LP Monitor initialized for {self.exchange}\n"
                            f"Price Change Trigger: {float(self.config.price_change_trigger_pct)}%\n"
                            f"Trigger Action: {self.config.trigger_action}")

        # Fetch positions on startup
        safe_ensure_future(self.initialize_monitoring())

    async def initialize_monitoring(self):
        """Fetch all positions owned by wallet and start monitoring"""
        await asyncio.sleep(3)  # Wait for connector to initialize

        try:
            connector = self.connectors[self.exchange]

            # Get wallet address
            wallet_address = connector.address
            self.logger().info(f"Fetching positions for wallet: {wallet_address}")

            # Fetch all positions owned by this wallet
            positions = await connector.get_user_positions()

            if not positions:
                self.logger().info("No positions found for this wallet")
                self.initialized = True
                return

            self.logger().info(f"Found {len(positions)} position(s)")

            # Initialize monitoring for each position
            for position in positions:
                await self._add_position_to_monitor(position)

            self.initialized = True
            self.logger().info(f"Now monitoring {len(self.monitored_positions)} position(s)")

        except Exception as e:
            self.logger().error(f"Error initializing monitoring: {str(e)}")
            self.initialized = True  # Mark as initialized to avoid retry loops

    async def _add_position_to_monitor(self, position: CLMMPositionInfo):
        """Add a position to the monitoring list"""
        try:
            # Fetch pool info for initial price
            connector = self.connectors[self.exchange]
            pool_info = await connector.get_pool_info(pool_address=position.pool_address)

            if pool_info:
                initial_price = Decimal(str(pool_info.price))
                self.monitored_positions[position.address] = MonitoredPosition(
                    position_info=position,
                    pool_info=pool_info,
                    initial_price=initial_price,
                    initial_time=time.time()
                )
                self.logger().info(
                    f"Monitoring position {position.address[:8]}... "
                    f"Initial price: {initial_price:.6f}"
                )
            else:
                self.logger().warning(f"Could not get pool info for position {position.address}")

        except Exception as e:
            self.logger().error(f"Error adding position to monitor: {str(e)}")

    def on_tick(self):
        """Called on each strategy tick"""
        if not self.initialized:
            return

        current_time = time.time()
        if current_time - self.last_update_time >= self.update_interval:
            self.last_update_time = current_time
            safe_ensure_future(self.update_and_check_positions())

    async def update_and_check_positions(self):
        """Update position info and check triggers"""
        if not self.monitored_positions:
            return

        connector = self.connectors[self.exchange]

        for position_address, monitored in list(self.monitored_positions.items()):
            # Skip if already triggered and action is alert (one-time notification)
            if monitored.triggered and self.config.trigger_action == "alert":
                continue

            # Skip if close is pending
            if monitored.pending_close_order_id:
                continue

            try:
                # Update pool info for current price
                pool_info = await connector.get_pool_info(pool_address=monitored.position_info.pool_address)
                if pool_info:
                    monitored.pool_info = pool_info

                    # Update position info
                    position_info = await connector.get_position_info(
                        trading_pair=monitored.position_info.trading_pair,
                        position_address=position_address
                    )
                    if position_info:
                        monitored.position_info = position_info

                    # Check price change trigger
                    await self._check_price_trigger(position_address, monitored)

            except Exception as e:
                self.logger().debug(f"Error updating position {position_address[:8]}...: {str(e)}")

    async def _check_price_trigger(self, position_address: str, monitored: MonitoredPosition):
        """Check if price change trigger is activated"""
        if not monitored.pool_info:
            return

        current_price = Decimal(str(monitored.pool_info.price))
        initial_price = monitored.initial_price

        # Calculate price change percentage
        if initial_price > 0:
            price_change_pct = abs((current_price - initial_price) / initial_price * 100)
        else:
            return

        # Check if trigger threshold is exceeded
        if price_change_pct >= self.config.price_change_trigger_pct:
            if not monitored.triggered:
                monitored.triggered = True
                await self._execute_trigger_action(position_address, monitored, price_change_pct)

    async def _execute_trigger_action(self, position_address: str, monitored: MonitoredPosition, price_change_pct: Decimal):
        """Execute the configured trigger action"""
        position = monitored.position_info
        base_token, quote_token = position.trading_pair.split("-") if position.trading_pair else ("BASE", "QUOTE")

        # Build notification message
        msg = (
            f"LP Monitor Alert!\n"
            f"Position: {position_address[:8]}...\n"
            f"Pool: {position.trading_pair}\n"
            f"Price Change: {float(price_change_pct):.2f}% vs Initial {float(monitored.initial_price):.6f}\n"
            f"Current Price: {float(monitored.pool_info.price):.6f}\n"
            f"Trigger: {float(self.config.price_change_trigger_pct)}%"
        )

        self.logger().warning(msg)
        self.notify_hb_app_with_timestamp(msg)

        if self.config.trigger_action == "close":
            await self._close_position(position_address, monitored)

    async def _close_position(self, position_address: str, monitored: MonitoredPosition):
        """Close the position"""
        try:
            self.logger().info(f"Auto-closing position {position_address[:8]}...")

            connector = self.connectors[self.exchange]
            order_id = connector.remove_liquidity(
                trading_pair=monitored.position_info.trading_pair,
                position_address=position_address
            )

            monitored.pending_close_order_id = order_id
            self.logger().info(f"Close order submitted with ID: {order_id}")

        except Exception as e:
            self.logger().error(f"Error closing position: {str(e)}")
            monitored.triggered = False  # Reset to allow retry

    def did_fill_order(self, event):
        """Called when an order is filled"""
        for position_address, monitored in list(self.monitored_positions.items()):
            if hasattr(event, 'order_id') and event.order_id == monitored.pending_close_order_id:
                self.logger().info(f"Position {position_address[:8]}... closed successfully")
                self.notify_hb_app_with_timestamp(f"Position {position_address[:8]}... closed")

                # Remove from monitoring
                del self.monitored_positions[position_address]
                break

    def _calculate_price_change(self, monitored: MonitoredPosition) -> tuple:
        """Calculate price change from initial price"""
        if not monitored.pool_info or monitored.initial_price == 0:
            return Decimal("0"), "unchanged"

        current_price = Decimal(str(monitored.pool_info.price))
        initial_price = monitored.initial_price

        change_pct = (current_price - initial_price) / initial_price * 100
        direction = "+" if change_pct >= 0 else ""

        return change_pct, direction

    def _calculate_position_width(self, position: CLMMPositionInfo) -> Decimal:
        """Calculate position width as percentage"""
        if position.lower_price == 0:
            return Decimal("0")

        mid_price = (Decimal(str(position.lower_price)) + Decimal(str(position.upper_price))) / 2
        width = (Decimal(str(position.upper_price)) - Decimal(str(position.lower_price))) / mid_price * 100
        return width

    def _get_position_status_icon(self, monitored: MonitoredPosition) -> str:
        """Get status icon based on position state"""
        if monitored.pending_close_order_id:
            return "Closing..."
        if monitored.triggered:
            return "TRIGGERED"

        if not monitored.pool_info:
            return "Unknown"

        current_price = Decimal(str(monitored.pool_info.price))
        lower = Decimal(str(monitored.position_info.lower_price))
        upper = Decimal(str(monitored.position_info.upper_price))

        if lower <= current_price <= upper:
            return "In Range"
        else:
            return "Out of Range"

    def format_status(self) -> str:
        """Format status message for display"""
        lines = []

        lines.append(f"LP Monitor - {self.exchange}")
        lines.append(f"Trigger: Price Change >= {float(self.config.price_change_trigger_pct)}%")
        lines.append(f"Action: {self.config.trigger_action.upper()}")
        lines.append("")

        if not self.initialized:
            lines.append("Initializing... fetching positions")
            return "\n".join(lines)

        if not self.monitored_positions:
            lines.append("No positions found to monitor")
            return "\n".join(lines)

        lines.append(f"Monitoring {len(self.monitored_positions)} position(s):")
        lines.append("")

        for position_address, monitored in self.monitored_positions.items():
            position = monitored.position_info
            base_token, quote_token = position.trading_pair.split("-") if position.trading_pair else ("BASE", "QUOTE")

            # Position header with status
            status = self._get_position_status_icon(monitored)
            lines.append(f"Position: {position_address[:12]}... [{status}]")

            # Pool info
            width = self._calculate_position_width(position)
            lines.append(f"  Pool: {position.trading_pair} ({self.exchange})")
            lines.append(f"  Range: {float(position.lower_price):.6f} - {float(position.upper_price):.6f} ({float(width):.1f}% Width)")

            # Current price and change
            if monitored.pool_info:
                current_price = Decimal(str(monitored.pool_info.price))
                change_pct, direction = self._calculate_price_change(monitored)

                lines.append(f"  Current Price: {float(current_price):.6f}")
                lines.append(f"  Price Change: {direction}{float(change_pct):.2f}% vs Initial {float(monitored.initial_price):.6f}")

            # Value calculation
            base_amount = Decimal(str(position.base_token_amount))
            quote_amount = Decimal(str(position.quote_token_amount))
            base_fee = Decimal(str(position.base_fee_amount))
            quote_fee = Decimal(str(position.quote_fee_amount))

            if monitored.pool_info:
                price = Decimal(str(monitored.pool_info.price))
                token_value = base_amount * price + quote_amount
                fee_value = base_fee * price + quote_fee
                total_value = token_value + fee_value

                lines.append(f"  Value: {float(total_value):.6f} {quote_token}")
                lines.append(f"  Tokens: {float(base_amount):.6f} {base_token} / {float(quote_amount):.6f} {quote_token}")

                if base_fee > 0 or quote_fee > 0:
                    lines.append(f"  Fees: {float(base_fee):.6f} {base_token} / {float(quote_fee):.6f} {quote_token}")

            # Trigger status
            if monitored.triggered:
                lines.append("  ALERT: Trigger activated!")
            else:
                change_pct, _ = self._calculate_price_change(monitored)
                remaining = self.config.price_change_trigger_pct - abs(change_pct)
                lines.append(f"  Trigger in: {float(remaining):.2f}% more price change")

            lines.append("")

        return "\n".join(lines)
