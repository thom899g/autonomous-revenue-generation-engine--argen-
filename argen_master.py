"""
ARGEN Master Agent - Core Orchestrator
Coordinates all revenue opportunity detection and execution modules.
Maintains system state, handles failures, and ensures continuous operation.
"""
import asyncio
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import traceback

from firebase_manager import FirebaseManager
from opportunity_scanner import OpportunityScanner
from revenue_opportunity import RevenueOpportunity
from config import CONFIG

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ArgenMaster:
    """Main orchestrator for Autonomous Revenue Generation Engine"""
    
    def __init__(self, firebase_creds_path: str = None):
        """Initialize the Master Agent with dependencies"""
        self.initialized = False
        self.running = False
        self.cycle_count = 0
        self.last_health_check = datetime.utcnow()
        
        # Initialize components
        try:
            self.firebase = FirebaseManager(firebase_creds_path)
            self.scanner = OpportunityScanner(self.firebase)
            self.active_opportunities: Dict[str, RevenueOpportunity] = {}
            self.failure_count = 0
            self.max_consecutive_failures = CONFIG['max_consecutive_failures']
            
            # Load persistent state
            self._load_state()
            self.initialized = True
            logger.info("ARGEN Master Agent initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize ARGEN Master: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    def _load_state(self) -> None:
        """Load persisted state from Firebase"""
        try:
            state_data = self.firebase.get_document('system_state', 'master_agent')
            if state_data:
                self.cycle_count = state_data.get('cycle_count', 0)
                self.failure_count = state_data.get('failure_count', 0)
                logger.info(f"Loaded state: {self.cycle_count} cycles, {self.failure_count} failures")
        except Exception as e:
            logger.warning(f"Could not load state, starting fresh: {str(e)}")
    
    def _save_state(self) -> None:
        """Save current state to Firebase"""
        try:
            state_data = {
                'cycle_count': self.cycle_count,
                'failure_count': self.failure_count,
                'last_updated': datetime.utcnow().isoformat(),
                'active_opportunities': len(self.active_opportunities),
                'status': 'running' if self.running else 'stopped'
            }
            self.firebase.set_document('system_state', 'master_agent', state_data)
        except Exception as e:
            logger.error(f"Failed to save state: {str(e)}")
    
    async def scan_opportunities(self) -> List[Dict[str, Any]]:
        """Scan for new revenue opportunities"""
        opportunities = []
        try:
            logger.info("Starting opportunity scan cycle...")
            
            # Run all scanners in parallel
            scan_tasks = []
            for scanner in self.scanner.get_scanners():
                scan_tasks.append(scanner.scan())
            
            # Wait for all scanners with timeout
            results = await asyncio.wait_for(
                asyncio.gather(*scan_tasks, return_exceptions=True),
                timeout=CONFIG['scan_timeout_seconds']
            )
            
            # Process results
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Scanner failed: {str(result)}")
                    continue
                if isinstance(result, list):
                    opportunities.extend(result)
                elif result:
                    opportunities.append(result)
            
            logger.info(f"Found {len(opportunities)} potential opportunities")
            
            # Filter and validate opportunities
            valid_opportunities = []
            for opp in opportunities:
                if self._validate_opportunity(opp):
                    valid_opportunities.append(opp)
            
            logger.info(f"{len(valid_opportunities)} opportunities passed validation")
            return valid_opportunities
            
        except asyncio.TimeoutError:
            logger.error("Opportunity scan timed out")
            return []
        except Exception as e:
            logger.error(f"Scan failed: {str(e)}")
            logger.error(traceback.format_exc())
            self.failure_count += 1
            return []
    
    def _validate_opportunity(self, opportunity: Dict[str, Any]) -> bool:
        """Validate an opportunity against business rules"""
        try:
            # Check required fields
            required_fields = ['type', 'potential_revenue', 'risk_score', 'execution_time']
            for field in required_fields:
                if field not in opportunity:
                    logger.warning(f"Opportunity missing required field: {field}")
                    return False
            
            # Validate types and ranges
            if not isinstance(opportunity['potential_revenue'], (int, float)):
                return False
            if opportunity['potential_revenue'] < CONFIG['min_revenue_threshold']:
                return False
            if opportunity['risk_score'] < 0 or opportunity['risk_score'] > 10:
                return False
            if opportunity['execution_time'] > CONFIG['max_execution_time_hours']:
                return False
            
            # Check for duplicates
            if self.firebase.opportunity_exists(opportunity):
                return False
            
            return True
            
        except Exception as e:
            logger.warning(f"Opportunity validation failed: {str(e)}")
            return False
    
    async def execute_opportunity(self, opportunity: Dict[str, Any]) -> bool:
        """Execute a validated revenue opportunity"""
        try:
            logger.info(f"Executing opportunity: {opportunity.get('type', 'unknown')}")
            
            # Create opportunity instance
            opp_instance = RevenueOpportunity.from_dict(opportunity)
            
            # Store in active tracking
            opp_id = opportunity.get('id', str(datetime.utcnow().timestamp()))
            self.active_opportunities[opp_id] = opp_instance
            
            # Execute with timeout
            result = await asyncio.wait_for(
                opp_instance.execute(),
                timeout=opportunity.get('execution_time', 3600)
            )
            
            # Process result
            if result.get('success', False):
                logger.info(f"Opportunity executed successfully: {result.get('revenue_actual', 0)}")
                
                # Record revenue
                revenue_data = {
                    'opportunity_id': opp_id,
                    'type': opportunity['type'],
                    'revenue_expected': opportunity['potential_revenue'],
                    'revenue_actual': result.get('revenue_actual', 0),
                    'execution_time': result.get('execution_time', 0),
                    'timestamp': datetime.utcnow().isoformat(),
                    'metadata': result.get('metadata', {})
                }
                self.firebase.record_revenue(revenue