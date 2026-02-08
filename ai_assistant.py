from __future__ import annotations

import sys
import threading
from typing import Optional

# OpenAI imports
try:
    from openai import OpenAI
    CLIENT = OpenAI()
except Exception:
    CLIENT = None

from pyxll import xl_macro, create_ctp, xl_app
from PySide6 import QtWidgets, QtCore, QtGui


# ============================================================================
# Worksheet Helper Functions
# ============================================================================

def get_worksheet_as_text():
    """Get the active worksheet data as plain text (tab-separated)."""
    try:
        app = xl_app()
        ws = app.ActiveSheet
        
        if ws is None:
            return "No active worksheet found."
        
        # Get used range
        used_range = ws.UsedRange
        if used_range is None:
            return "Worksheet is empty."
        
        # Get the data
        values = used_range.Value
        
        if values is None:
            return "Worksheet is empty."
        
        # Normalize COM return value to list of lists
        # COM can return: scalar, 1D list (row/column), or 2D list
        if not isinstance(values, (list, tuple)):
            # Single cell - wrap in list
            data = [[values]]
        elif len(values) == 0:
            # Empty range
            return "Worksheet is empty."
        else:
            # Check if first element is a list/tuple (indicating 2D structure)
            first_elem = values[0] if len(values) > 0 else None
            if isinstance(first_elem, (list, tuple)):
                # 2D array - convert tuples to lists if needed
                data = [list(row) if isinstance(row, tuple) else row for row in values]
            else:
                # Single row - wrap in list
                # Convert tuple to list if needed
                data = [list(values) if isinstance(values, tuple) else values]
        
        # Convert to text format (tab-separated, newline-separated rows)
        lines = []
        for row in data:
            # Convert each cell to string, handle None
            row_str = []
            for cell in row:
                if cell is None:
                    row_str.append("")
                else:
                    row_str.append(str(cell))
            lines.append("\t".join(row_str))
        
        return "\n".join(lines)
    
    except Exception as e:
        return f"Error reading worksheet: {str(e)}"


# ============================================================================
# Qt Bootstrap
# ============================================================================

def _ensure_qt():
    """Ensure Qt application exists."""
    global _qt_app
    app = QtWidgets.QApplication.instance()
    if app is None:
        app = QtWidgets.QApplication([])
    _qt_app = app
    return app


# ============================================================================
# Loading Spinner Widget
# ============================================================================

class LoadingSpinner(QtWidgets.QLabel):
    """A rotating loading spinner widget."""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedSize(20, 20)
        self.setAlignment(QtCore.Qt.AlignCenter)
        self._angle = 0
        self._timer = QtCore.QTimer(self)
        self._timer.timeout.connect(self._rotate)
        self._timer.setInterval(50)  # Update every 50ms for smooth rotation
        self.hide()
        
        self.setStyleSheet("""
            QLabel {
                color: #4a9a4a;
                font-size: 16px;
                background: transparent;
            }
        """)
    
    def _rotate(self):
        """Rotate the spinner."""
        self._angle = (self._angle + 15) % 360
        self.update()  # Trigger repaint
    
    def paintEvent(self, event):
        """Draw the rotating spinner."""
        painter = QtGui.QPainter(self)
        try:
            painter.setRenderHint(QtGui.QPainter.Antialiasing)
            painter.translate(self.width() / 2, self.height() / 2)
            painter.rotate(self._angle)
            painter.translate(-self.width() / 2, -self.height() / 2)
            
            # Draw spinner as a circle with a gap
            pen = QtGui.QPen(QtGui.QColor("#4a9a4a"), 2)
            painter.setPen(pen)
            painter.setBrush(QtCore.Qt.NoBrush)
            rect = QtCore.QRect(4, 4, 12, 12)
            painter.drawArc(rect, 0, 270 * 16)  # Draw 3/4 circle
        finally:
            painter.end()
    
    def start(self):
        """Start the spinner animation."""
        self._angle = 0
        self._timer.start()
        self.show()
    
    def stop(self):
        """Stop the spinner animation."""
        self._timer.stop()
        self.hide()


# ============================================================================
# Chat Widget
# ============================================================================

class ChatWidget(QtWidgets.QWidget):
    """Simple chat widget for interacting with AI model about workbook data."""
    
    # Signal for thread-safe UI updates
    response_ready = QtCore.Signal(str, str)  # error_msg, ai_response
    
    def __init__(self):
        super().__init__()
        
        # Store chat history and conversation
        self.conversation = None
        self._init_conversation()
        
        # Store chat history for display
        self.chat_history = []
        self._thinking_msg_id = None
        
        # Connect signal for thread-safe UI updates
        self.response_ready.connect(self._update_ui_safe)
        
        # Set light background
        self.setStyleSheet("""
            QWidget {
                background-color: #f5f5f5;
                color: black;
            }
        """)
        
        # Title label
        title = QtWidgets.QLabel("PyFi Spreadsheet Assistant")
        title.setAlignment(QtCore.Qt.AlignCenter)
        title.setStyleSheet("""
            QLabel {
                color: #ffffff;
                font-size: 14px;
                font-weight: bold;
                padding: 6px;
                background-color: #1e3a5f;
                border: 2px solid #2a4a6f;
                border-radius: 6px;
            }
        """)
        
        # Chat history display
        self.chat_display = QtWidgets.QTextEdit()
        self.chat_display.setReadOnly(True)
        self.chat_display.setFont(QtGui.QFont("Consolas", 9))
        self.chat_display.setStyleSheet("""
            QTextEdit {
                background-color: #fafafa;
                color: black;
                border: 1px solid #d0d0d0;
                border-radius: 4px;
                padding: 8px;
            }
        """)
        
        # Input field
        self.input_field = QtWidgets.QLineEdit()
        self.input_field.setPlaceholderText("Type your question here...")
        self.input_field.setStyleSheet("""
            QLineEdit {
                background-color: #ffffff;
                color: black;
                border: 1px solid #d0d0d0;
                border-radius: 4px;
                padding: 8px;
                font-size: 11px;
            }
        """)
        self.input_field.returnPressed.connect(self.send_message)
        
        # Send button
        self.send_btn = QtWidgets.QPushButton("Send")
        self.send_btn.clicked.connect(self.send_message)
        self.send_btn.setStyleSheet("""
            QPushButton {
                background-color: #4a9a4a;
                color: white;
                font-size: 11px;
                font-weight: bold;
                padding: 8px 20px;
                border-radius: 4px;
                min-width: 80px;
            }
            QPushButton:hover {
                background-color: #5aaa5a;
            }
            QPushButton:disabled {
                background-color: #e0e0e0;
                color: #999999;
            }
        """)
        
        # Loading spinner widget
        self.loading_spinner = LoadingSpinner(self)
        
        # Model selection dropdown
        model_label = QtWidgets.QLabel("AI Model:")
        model_label.setStyleSheet("""
            QLabel {
                color: #333333;
                font-size: 10px;
                font-weight: bold;
            }
        """)
        
        self.model_combo = QtWidgets.QComboBox()
        self.model_combo.addItems([
            "gpt-5.2",
            "gpt-5-mini",
            "gpt-5-nano",
            "gpt-5.2-pro",
            "gpt-5",
            "gpt-4.1"
        ])
        self.model_combo.setCurrentText("gpt-5-mini")  # Default model
        self.model_combo.setStyleSheet("""
            QComboBox {
                background-color: #ffffff;
                color: black;
                border: 1px solid #d0d0d0;
                border-radius: 4px;
                padding: 4px 8px;
                font-size: 10px;
                min-width: 120px;
            }
            QComboBox:hover {
                border: 1px solid #4a9a4a;
            }
            QComboBox::drop-down {
                border: none;
                width: 20px;
            }
        """)
        
        # Clear button
        self.clear_btn = QtWidgets.QPushButton("Clear Chat")
        self.clear_btn.clicked.connect(self.clear_chat)
        self.clear_btn.setStyleSheet("""
            QPushButton {
                background-color: #e0e0e0;
                color: #333333;
                font-size: 10px;
                font-weight: bold;
                padding: 6px 15px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #d0d0d0;
            }
        """)
        
        # Layout
        layout = QtWidgets.QVBoxLayout(self)
        layout.addWidget(title)
        
        # Model selection row
        model_layout = QtWidgets.QHBoxLayout()
        model_layout.addWidget(model_label)
        model_layout.addWidget(self.model_combo)
        model_layout.addStretch()
        layout.addLayout(model_layout)
        
        layout.addWidget(self.chat_display)
        
        # Input row
        input_layout = QtWidgets.QHBoxLayout()
        input_layout.addWidget(self.input_field)
        input_layout.addWidget(self.loading_spinner)
        input_layout.addWidget(self.send_btn)
        layout.addLayout(input_layout)
        
        # Clear button at the bottom
        layout.addWidget(self.clear_btn)
        
        # Add welcome message
        self.add_message("Excel Assistant", "AI Chat Assistant ready. Testing with hardcoded message...", is_system=True)
        
        # Automatically send test message after widget is ready
        QtCore.QTimer.singleShot(500, self._send_test_message)
    
    def _init_conversation(self):
        """Initialize OpenAI conversation."""
        try:
            if CLIENT is not None:
                self.conversation = CLIENT.conversations.create()
                print(f"[DEBUG] Created conversation: {self.conversation.id}")
        except Exception as e:
            print(f"[DEBUG] Error creating conversation: {e}")
            self.conversation = None
    
    def _format_message_text(self, text: str) -> str:
        """Format message text for HTML display, preserving newlines and formatting."""
        import html
        
        # Escape HTML special characters first
        text = html.escape(text)
        
        # Split into lines to process each line separately
        lines = text.split('\n')
        formatted_lines = []
        
        for line in lines:
            if not line.strip():
                # Empty line - add a line break
                formatted_lines.append('<br>')
            else:
                # Preserve leading spaces by converting to non-breaking spaces
                # This helps with code indentation
                leading_spaces = len(line) - len(line.lstrip(' '))
                if leading_spaces > 0:
                    # Replace leading spaces with non-breaking spaces
                    formatted_line = '&nbsp;' * leading_spaces + line.lstrip(' ')
                    formatted_lines.append(formatted_line)
                else:
                    formatted_lines.append(line)
        
        # Join with <br> tags to preserve line breaks
        return '<br>'.join(formatted_lines)
    
    def add_message(self, sender: str, message: str, is_system: bool = False):
        """Add a message to the chat display."""
        if is_system:
            # Format the message text to preserve newlines
            formatted_text = self._format_message_text(message)
            formatted = f'<span style="color: #d97706; font-weight: normal;">[{sender}]:</span> <span style="color: black;">{formatted_text}</span>'
        else:
            color = "#4a9a4a" if sender == "AI" else "#2563eb"
            # Format the message text to preserve newlines
            formatted_text = self._format_message_text(message)
            formatted = f'<span style="color: {color}; font-weight: bold;">[{sender}]:</span> <span style="color: black;">{formatted_text}</span>'
        
        self.chat_display.append(formatted)
        
        # Auto-scroll to bottom
        scrollbar = self.chat_display.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())
    
    def clear_chat(self):
        """Clear the chat history."""
        self.chat_display.clear()
        self.chat_history = []
        self._init_conversation()  # Create new conversation
        self.add_message("Excel Assistant", "Chat cleared.", is_system=True)
    
    def _send_test_message(self):
        """Send a hardcoded test message to AI."""
        print("[DEBUG] _send_test_message() called")
        
        message = "Hello How are you?"
        print(f"[DEBUG] Hardcoded message: {message}")
        
        # Add user message to display
        self.add_message("You", message)
        print("[DEBUG] User message added to display")
        
        # Show loading spinner
        self.loading_spinner.start()
        print("[DEBUG] Loading spinner started")
        
        # Call AI directly (synchronously for testing)
        ai_response = None
        error_msg = None
        
        try:
            print("[DEBUG] Checking CLIENT...")
            if CLIENT is None:
                raise Exception("OpenAI client not initialized.")
            
            print("[DEBUG] Checking conversation...")
            # Initialize conversation if needed
            if self.conversation is None:
                print("[DEBUG] Conversation is None - initializing...")
                self._init_conversation()
                if self.conversation is None:
                    raise Exception("Failed to create conversation.")
                print(f"[DEBUG] Conversation created: {self.conversation.id}")
            else:
                print(f"[DEBUG] Using existing conversation: {self.conversation.id}")
            
            # Get selected model
            selected_model = self.model_combo.currentText()
            
            print(f"[DEBUG] Calling OpenAI API with conversation {self.conversation.id}...")
            print(f"[DEBUG] Message: {message}")
            print(f"[DEBUG] Model: {selected_model}, Service tier: priority")
            
            # Get worksheet data as context
            worksheet_text = get_worksheet_as_text()
            print(f"[DEBUG] Worksheet context length: {len(worksheet_text)} characters")
            
            # Combine worksheet context with user message and system prompt
            full_message = f"System: Responses should be brief to fit a small window.\n\nCurrent worksheet data:\n{worksheet_text}\n\nUser question: {message}"
            
            # Use the same API pattern as demo notebook
            response = CLIENT.responses.create(
                conversation=self.conversation.id,
                model=selected_model,
                input=full_message,
                service_tier="priority"
            )
            
            print(f"[DEBUG] API call completed")
            print(f"[DEBUG] Response object: {type(response)}")
            print(f"[DEBUG] Response attributes: {dir(response)}")
            
            ai_response = response.output_text
            print(f"[DEBUG] Got response text: {ai_response[:100] if ai_response else 'None'}...")
            print(f"[DEBUG] Response length: {len(ai_response) if ai_response else 0}")
        
        except Exception as e:
            error_msg = f"Error: {str(e)}"
            print(f"[DEBUG] EXCEPTION: {error_msg}")
            import traceback
            print("[DEBUG] Full traceback:")
            traceback.print_exc()
        
        # Update UI directly
        print("[DEBUG] ===== Updating UI =====")
        print(f"[DEBUG] error_msg: {error_msg}")
        print(f"[DEBUG] ai_response exists: {bool(ai_response)}")
        if ai_response:
            print(f"[DEBUG] ai_response preview: {ai_response[:100]}")
        
        try:
            if error_msg:
                print(f"[DEBUG] Showing error message in UI")
                self.add_message("Excel Assistant", error_msg, is_system=True)
            elif ai_response:
                print(f"[DEBUG] Showing AI response in UI")
                self.add_message("Excel Assistant", ai_response, is_system=True)
            else:
                print("[DEBUG] No response received - showing message")
                self.add_message("Excel Assistant", "No response received.", is_system=True)
            
            print("[DEBUG] Message added to UI")
            
            # Stop loading spinner
            self.loading_spinner.stop()
            print("[DEBUG] Loading spinner stopped")
        except Exception as e:
            print(f"[DEBUG] ERROR in UI update: {e}")
            import traceback
            traceback.print_exc()
            # Stop spinner on error
            try:
                self.loading_spinner.stop()
            except:
                pass
        
        print("[DEBUG] ===== Test complete =====")
        
    def send_message(self):
        """Send a message to the AI model."""
        print("[DEBUG] send_message() called")
        
        if CLIENT is None:
            print("[DEBUG] CLIENT is None - showing error")
            self.add_message("Excel Assistant", "OpenAI client not available.", is_system=True)
            return
        
        # Capture message BEFORE clearing input - CRITICAL
        message = self.input_field.text().strip()
        print(f"[DEBUG] ===== MESSAGE CAPTURE =====")
        print(f"[DEBUG] Input field object: {self.input_field}")
        print(f"[DEBUG] Input field text(): '{self.input_field.text()}'")
        print(f"[DEBUG] Message after strip: '{message}'")
        print(f"[DEBUG] Message length: {len(message)}")
        
        if not message:
            print("[DEBUG] Empty message - returning")
            return
        
        print(f"[DEBUG] Processing message: '{message}'")
        
        # Clear input immediately
        self.input_field.clear()
        
        # Disable send button
        self.send_btn.setEnabled(False)
        print("[DEBUG] Send button disabled")
        
        # Show loading spinner
        self.loading_spinner.start()
        print("[DEBUG] Loading spinner started")
        
        # Add user message to display
        self.add_message("You", message)
        print("[DEBUG] User message added to display")
        
        # Call AI in a separate thread (non-blocking)
        def get_response():
            print("[DEBUG] get_response() thread started")
            ai_response = None
            error_msg = None
            
            try:
                print("[DEBUG] Checking CLIENT...")
                if CLIENT is None:
                    raise Exception("OpenAI client not initialized.")
                
                print("[DEBUG] Checking conversation...")
                # Initialize conversation if needed
                if self.conversation is None:
                    print("[DEBUG] Conversation is None - initializing...")
                    self._init_conversation()
                    if self.conversation is None:
                        raise Exception("Failed to create conversation.")
                    print(f"[DEBUG] Conversation created: {self.conversation.id}")
                else:
                    print(f"[DEBUG] Using existing conversation: {self.conversation.id}")
                
                # Get selected model
                selected_model = self.model_combo.currentText()
                
                print(f"[DEBUG] Calling OpenAI API with conversation {self.conversation.id}...")
                print(f"[DEBUG] Message: {message}")
                print(f"[DEBUG] Model: {selected_model}, Service tier: priority")
                
                # Get worksheet data as context
                worksheet_text = get_worksheet_as_text()
                print(f"[DEBUG] Worksheet context length: {len(worksheet_text)} characters")
                
                # Combine worksheet context with user message and system prompt
                full_message = f"System: Responses should be brief to fit a small window.\n\nCurrent worksheet data:\n{worksheet_text}\n\nUser question: {message}"
                
                # Use the same API pattern as demo notebook
                response = CLIENT.responses.create(
                    conversation=self.conversation.id,
                    model=selected_model,
                    input=full_message,
                    service_tier="priority"
                )
                
                print(f"[DEBUG] API call completed")
                print(f"[DEBUG] Response object: {type(response)}")
                
                ai_response = response.output_text
                print(f"[DEBUG] Got response text: {ai_response[:100] if ai_response else 'None'}...")
                print(f"[DEBUG] Response length: {len(ai_response) if ai_response else 0}")
                
            except Exception as e:
                error_msg = f"Error: {str(e)}"
                print(f"[DEBUG] EXCEPTION in get_response: {error_msg}")
                import traceback
                print("[DEBUG] Full traceback:")
                traceback.print_exc()
            
            print(f"[DEBUG] Thread finished - ai_response: {bool(ai_response)}, error_msg: {bool(error_msg)}")
            
            # Use signal for thread-safe UI update (Qt signals work from threads)
            print("[DEBUG] Emitting response_ready signal for thread-safe UI update")
            try:
                error_str = error_msg if error_msg else ""
                response_str = ai_response if ai_response else ""
                self.response_ready.emit(error_str, response_str)
                print(f"[DEBUG] Signal emitted - error: '{error_str[:50]}', response: '{response_str[:50] if response_str else 'None'}'")
            except Exception as e:
                print(f"[DEBUG] ERROR emitting signal: {e}")
                import traceback
                traceback.print_exc()
                # Last resort: try to re-enable button directly (may not be thread-safe)
                try:
                    # This might not work from thread, but worth trying
                    QtCore.QMetaObject.invokeMethod(
                        self.send_btn,
                        "setEnabled",
                        QtCore.Qt.QueuedConnection,
                        QtCore.Q_ARG(bool, True)
                    )
                except:
                    pass
        
        print("[DEBUG] Starting thread...")
        try:
            thread = threading.Thread(target=get_response, daemon=True)
            thread.start()
            print(f"[DEBUG] Thread started: {thread.is_alive()}")
        except Exception as e:
            print(f"[DEBUG] ERROR starting thread: {e}")
            import traceback
            traceback.print_exc()
            # Re-enable button and stop spinner on error
            self.loading_spinner.stop()
            self.send_btn.setEnabled(True)
    
    @QtCore.Slot(str, str)
    def _update_ui_safe(self, error_msg: str, ai_response: str):
        """Thread-safe UI update method."""
        print("[DEBUG] _update_ui_safe() called")
        try:
            if error_msg:
                print(f"[DEBUG] Showing error message in UI")
                self.add_message("Excel Assistant", error_msg, is_system=True)
            elif ai_response:
                print(f"[DEBUG] Showing AI response in UI")
                self.add_message("Excel Assistant", ai_response, is_system=True)
            else:
                print("[DEBUG] No response received - showing message")
                self.add_message("Excel Assistant", "No response received.", is_system=True)
            
            # Stop loading spinner
            self.loading_spinner.stop()
            print("[DEBUG] Loading spinner stopped")
            
            # Re-enable send button
            self.send_btn.setEnabled(True)
            print("[DEBUG] Send button re-enabled in _update_ui_safe")
        except Exception as e:
            print(f"[DEBUG] ERROR in _update_ui_safe: {e}")
            import traceback
            traceback.print_exc()
            # Make sure button is enabled and spinner stopped even on error
            try:
                self.loading_spinner.stop()
                self.send_btn.setEnabled(True)
            except:
                pass


# ============================================================================
# Macros
# ============================================================================

_chat_ctp = None

@xl_macro
def show_ai_chat():
    """Show the AI chat widget in a Custom Task Pane."""
    global _chat_ctp
    
    try:
        _ensure_qt()
        
        if _chat_ctp is None:
            widget = ChatWidget()
            _chat_ctp = create_ctp(widget, width=500)
        
        return "AI Assistant loaded successfully!"
    except Exception as e:
        import traceback
        error_msg = f"Error showing AI chat: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        return error_msg
