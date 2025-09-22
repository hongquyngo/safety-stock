# app.py
"""
Safety Stock Management System
Main entry point with authentication
"""

import streamlit as st
from utils.auth import AuthManager
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="Safety Stock Management",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Initialize auth manager
auth_manager = AuthManager()

def main():
    """Main application entry point"""
    
    # Check if user is already authenticated
    if auth_manager.check_session():
        show_authenticated_content()
    else:
        show_login_page()

def show_login_page():
    """Display login page"""
    
    # Custom CSS for login page
    st.markdown("""
    <style>
    .main > div {
        padding-top: 5rem;
    }
    .stButton > button {
        width: 100%;
        background-color: #0066CC;
        color: white;
    }
    .stButton > button:hover {
        background-color: #0052A3;
        color: white;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Center the login form
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("## 📦 Safety Stock Management System")
        st.markdown("---")
        
        with st.form("login_form", clear_on_submit=False):
            st.markdown("### Sign In")
            
            username = st.text_input(
                "Username",
                placeholder="Enter your username",
                key="login_username"
            )
            
            password = st.text_input(
                "Password",
                type="password",
                placeholder="Enter your password",
                key="login_password"
            )
            
            col_btn1, col_btn2 = st.columns(2)
            with col_btn1:
                submitted = st.form_submit_button(
                    "🔐 Login",
                    use_container_width=True,
                    type="primary"
                )
            
            if submitted and username and password:
                with st.spinner("Authenticating..."):
                    success, result = auth_manager.authenticate(username, password)
                    
                    if success:
                        auth_manager.login(result)
                        st.success("✅ Login successful! Redirecting...")
                        st.rerun()
                    else:
                        error_msg = result.get("error", "Authentication failed")
                        st.error(f"❌ {error_msg}")
            elif submitted:
                st.warning("⚠️ Please enter both username and password")
        
        # Info section
        with st.expander("ℹ️ System Information"):
            st.markdown("""
            **Safety Stock Management Features:**
            - View and manage safety stock levels
            - Configure calculation methods
            - Track review history
            - Bulk import/export capabilities
            - Customer-specific overrides
            
            **Support:** Contact IT Support for login issues
            """)

def show_authenticated_content():
    """Show content for authenticated users"""
    
    # Sidebar with user info
    with st.sidebar:
        st.markdown("### 👤 User Information")
        st.markdown(f"**User:** {auth_manager.get_user_display_name()}")
        st.markdown(f"**Role:** {st.session_state.get('user_role', 'User')}")
        
        st.markdown("---")
        
        if st.button("🚪 Logout", use_container_width=True):
            auth_manager.logout()
            st.rerun()
    
    # Main content area
    st.markdown("# 📦 Safety Stock Management System")
    st.markdown("---")
    
    # Navigation instructions
    col1, col2 = st.columns([3, 1])
    with col1:
        st.info("""
        👈 **Navigate to Safety Stock Management** in the sidebar to access the main application.
        
        This system helps you:
        - Maintain optimal inventory levels
        - Prevent stockouts
        - Minimize excess inventory
        - Track performance metrics
        """)
    
    with col2:
        # Quick stats placeholder
        st.metric("Active SKUs", "---", help="Total products with safety stock rules")
        st.metric("Pending Reviews", "---", help="Items requiring review")
    
    # Quick actions
    st.markdown("### Quick Links")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        📊 **Dashboard**
        - View key metrics
        - Monitor alerts
        """)
    
    with col2:
        st.markdown("""
        📦 **Safety Stock**
        - Manage stock levels
        - Configure rules
        """)
    
    with col3:
        st.markdown("""
        📈 **Reports**
        - Export data
        - Review history
        """)

if __name__ == "__main__":
    main()