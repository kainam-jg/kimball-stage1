#!/usr/bin/env python3
"""
Authentication Utilities for Streamlit Application
Provides authentication checking and session management
"""

import streamlit as st
import json
import os

def check_authentication():
    """Check if user is authenticated, redirect to login if not."""
    if 'authenticated' not in st.session_state or not st.session_state.authenticated:
        st.error("🔐 Authentication required")
        st.markdown("Please log in to access this page.")
        
        # Create a login form directly on the page
        st.markdown("### Login")
        with st.form("login_form"):
            username = st.text_input("Username", placeholder="Enter your username")
            password = st.text_input("Password", type="password", placeholder="Enter your password")
            submit_button = st.form_submit_button("🔓 Login", type="primary")
        
        if submit_button:
            if not username or not password:
                st.error("❌ Please enter both username and password")
            else:
                # Load config and check credentials
                auth_config = load_auth_config()
                if auth_config:
                    stored_username = auth_config.get('username')
                    stored_password = auth_config.get('password')
                    
                    if username == stored_username and password == stored_password:
                        st.session_state.authenticated = True
                        st.session_state.username = username
                        st.success("✅ Authentication successful!")
                        st.rerun()
                    else:
                        st.error("❌ Invalid username or password")
                else:
                    st.error("❌ Authentication not configured")
        
        st.stop()

def get_current_user():
    """Get the currently authenticated user's username."""
    return st.session_state.get('username', 'Unknown')

def logout():
    """Log out the current user."""
    if 'authenticated' in st.session_state:
        del st.session_state.authenticated
    if 'username' in st.session_state:
        del st.session_state.username
    st.success("✅ Logged out successfully")
    st.rerun()

def show_user_info():
    """Display current user information in the sidebar."""
    if 'authenticated' in st.session_state and st.session_state.authenticated:
        st.sidebar.markdown("---")
        st.sidebar.markdown(f"👤 **User:** {get_current_user()}")
        
        if st.sidebar.button("🚪 Logout", help="Log out of the application"):
            logout()

def load_auth_config():
    """Load authentication configuration from config.json."""
    config_file = "config.json"
    if not os.path.exists(config_file):
        return None
    
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
            return config.get('authentication', {})
    except Exception:
        return None

def is_auth_configured():
    """Check if authentication is configured in config.json."""
    auth_config = load_auth_config()
    if not auth_config:
        return False
    
    username = auth_config.get('username')
    password = auth_config.get('password')
    
    return bool(username and password)
