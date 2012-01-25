;;--------------------------------------------------------------------------
;;  Copyright 2009 Taro L. Saito
;;
;;   Licensed under the Apache License, Version 2.0 (the "License");
;;   you may not use this file except in compliance with the License.
;;   You may obtain a copy of the License at
;; 
;;      http://www.apache.org/licenses/LICENSE-2.0
;; 
;;   Unless required by applicable law or agreed to in writing, software
;;   distributed under the License is distributed on an "AS IS" BASIS,
;;   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;;   See the License for the specific language governing permissions and
;;   limitations under the License.
;;--------------------------------------------------------------------------
;;---------------------------
;; silk-mode.el
;;
;; Created by: Taro L. Saito
;; Since: Aug 30, 2009
;;
;;--------------------------- 

(defconst silk-mode-revision "$Revision: 855 $")
 
(defconst silk-mode-version
  (progn
   (string-match "[0-9.]+" silk-mode-revision)
   (substring silk-mode-revision (match-beginning 0) (match-end 0))))


(defun silk-mode ()
  "Major mode for editing Silk format data in Emacs."
  (interactive)
  (kill-all-local-variables)
  (setq major-mode 'silk-mode)
  (setq mode-name "Silk")
  (make-variable-buffer-local 'comment-start)
  (setq comment-start "# ")
  (make-variable-buffer-local 'comment-end)
  (setq comment-end "")
  (make-variable-buffer-local 'comment-start-skip)
  (setq comment-start-skip "#+ *")
  (make-local-variable 'parse-sexp-ignore-comments)
  (setq parse-sexp-ignore-comments t)
  (set-syntax-table silk-mode-syntax-table)
  (use-local-map silk-mode-map)
  (set (make-local-variable 'font-lock-defaults) 
       '(silk-font-lock-keywords nil nil))
  (run-hooks 'silk-mode-hook))





(defun wrap(&rest l)
  (concat "\\(" (apply 'concat l) "\\)"))

; (indent) (node name) "(" (node name)(:node value)? ("," (node name)(:node value)?)* ")" (: node value)?
(defun silk-node-regexp
  (let ((indent "^ *[->]+")
	(node "[A-Za-z0-9_ ]+")
	(value ": *[^,:()\n]+"))
   (concat indent (wrap node) (wrap "(" (wrap node) (wrap value)"?" (wrap "," (wrap node) (wrap value)"?" )"*"  ")")"?")))


(defconst silk-qname "\\([A-Za-z_\\.][A-Za-z0-9_\\. ]*\\)")
(defconst silk-node-expr (concat "^ *\\([->]+\\) *" silk-qname "? *\\([-(]\\)"))
(defconst silk-node-value "\\(: *\\([^,:()]+\\)\\)?\\)")

(defun my-font-lock-restart ()
  (interactive)
  (setq font-lock-mode-major-mode nil)
  (set 'font-lock-defaults
       '(silk-font-lock-keywords nil nil))
  (font-lock-fontify-buffer))

; Do 'M-C-x' and 'M-x load silk-mode' to reload the configuration
(defvar silk-font-lock-keywords
  (list
   ; node 
   '("^ *\\(-[->|*]?\\) *\\([A-Za-z_\.][A-Za-z0-9_\. ]*\\)?" (1 font-lock-builtin-face) (2 font-lock-keyword-face t t)    
  ; attribute name
     ("\\(- \\|[,(]\\) *\\([A-Za-z_\.][A-Za-z0-9_\. ]*\\)" nil nil (1 font-lock-builtin-face nil t) (2 font-lock-variable-name-face nil t)) 
    ; node value
;     ("\\(:\\)\\([<>@#A-Za-z0-9\. +-]*\\)" nil nil (1 font-lock-builtin-face) (2 font-lock-builtin-face t t))
	 )
   ; context node
   '("^ *\\(=\\) *\\([A-Za-z_\.][A-Za-z0-9_\. ]*\\)?" (1 font-lock-builtin-face) (2 font-lock-variable-name-face nil t)
  ; attribute name
     ("\\(- \\|[-,(]\\) *\\([A-Za-z_\.][A-Za-z0-9_\. ]*\\)" nil nil (1 font-lock-builtin-face nil t) (2 font-lock-keyword-face nil t)))
   ; preamble
   '("^ *\\(%[A-Za-z_\.][A-Za-z0-9_\.]*\\)" (1 font-lock-preprocessor-face t) 
  ; attribute name
     ("\\( \\|[-,(]\\) *\\([A-Za-z_\.][A-Za-z0-9_\. ]*\\)" nil nil (2 font-lock-builtin-face nil t)) 
    ; node value
;     ("\\(:\\)\\([<>@#A-Za-z0-9\. +-]*\\)" nil nil (1 font-lock-builtin-face) (2 font-lock-builtin-face t t))
    )
   ; type name (or XML tag)
   '("\\[\\([A-Za-z0-9\._ ]*\\)\\( *\\(,\\)\\([A-Za-z0-9\._ ]*\\)\\)*\\]" (1 font-lock-type-face t t)  (4 font-lock-type-face t t))
   ; function
   '("\\(@[A-Za-z_\.][A-Za-z0-9_\.]+\\)" (1 font-lock-function-name-face nil t)
  ; function argument name
     ("\\(- \\|[,(]\\) *\\([A-Za-z_\.][A-Za-z0-9_\. ]*\\)" nil nil (1 font-lock-builtin-face nil t) (2 font-lock-variable-name-face nil t))
   ; function argument value
    ("\\(:\\)\\([#A-Za-z0-9\. +-]*\\)" nil nil (1 font-lock-builtin-face) (2 font-lock-builtin-face t t))
     )
   ; string
   '("\\(\\\"\\([^\"]\\|\\\\\"\\)*\\\"\\)" (1 font-lock-string-face t))
   ; comment
   '("^ *#\\(.*\\)$" (1 font-lock-comment-face t))
   ))

(defvar silk-mode-syntax-table nil
  "Syntax table in use in silk-mode buffers.")

(if silk-mode-syntax-table
    ()
  (progn
    (setq silk-mode-syntax-table (make-syntax-table))
    (modify-syntax-entry ?\" "\"" silk-mode-syntax-table)
;    (modify-syntax-entry ?\' "\"" silk-mode-syntax-table)
    (modify-syntax-entry ?# "<" silk-mode-syntax-table)
    (modify-syntax-entry ?\n ">" silk-mode-syntax-table)
    (modify-syntax-entry ?\\ "\\" silk-mode-syntax-table)
    (modify-syntax-entry ?- "." silk-mode-syntax-table)
    (modify-syntax-entry ?\( "." silk-mode-syntax-table)
    (modify-syntax-entry ?\) "." silk-mode-syntax-table)
    (modify-syntax-entry ?\{ "(}" silk-mode-syntax-table)
    (modify-syntax-entry ?\} "){" silk-mode-syntax-table)
    (modify-syntax-entry ?\[ "(]" silk-mode-syntax-table)
    (modify-syntax-entry ?\] ")[" silk-mode-syntax-table)))



(defvar silk-font-lock-syntactic-keywords nil)


(defun silk-node ()
  (interactive)
  (insert "-node()"))


(defvar silk-mode-map
  (let ((map (make-sparse-keymap)))
;    (define-key map "(" 'silk-electric-brace)
;    (define-key map ")" 'silk-electric-brace)
    (define-key map (kbd "TAB")   'silk-indent-line)
    map)
  "Keymap used in Silk mode.")

(defun silk-electric-brace (arg)
  "Insert a brace and re-indent the current line."
  (interactive "P")
  (self-insert-command (prefix-numeric-value arg))
  (silk-indent-line t))

(defun silk-current-indentation ()
  "Return the indentation level of current line."
  (save-excursion
    (beginning-of-line)
    (back-to-indentation)
    (current-column)))


(defun silk-indent-line (&optional flag)
  "Correct the indentation of the current Silk line."
  (interactive)
  (insert "	"))

(defun silk-indent-to (column)
  "Indent the current line to COLUMN."
  (when column
    (let (shift top beg)
      (and (< column 0) (error "invalid nest"))
      (setq shift (current-column))
      (beginning-of-line)
      (setq beg (point))
      (back-to-indentation)
      (setq top (current-column))
      (skip-chars-backward " \t")
      (if (>= shift top) (setq shift (- shift top))
        (setq shift 0))
      (if (and (bolp)
               (= column top))
          (move-to-column (+ column shift))
        (move-to-column top)
        (delete-region beg (point))
        (beginning-of-line)
        (indent-to column)
        (move-to-column (+ column shift))))))




;;;###autoload
(add-to-list 'auto-mode-alist '("\\.silk\\'" . silk-mode))


(provide 'silk-mode)

