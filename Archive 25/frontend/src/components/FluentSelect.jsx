import { useState, useRef, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { FiChevronDown, FiCheck } from 'react-icons/fi';

export default function FluentSelect({ 
  value, onChange, options = [], placeholder = "Select...", 
  style, className = "", multi = false 
}) {
  const [isOpen, setIsOpen] = useState(false);
  const containerRef = useRef(null);

  useEffect(() => {
    function handleClickOutside(e) {
      if (containerRef.current && !containerRef.current.contains(e.target)) {
        setIsOpen(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const isSelected = (val) => {
    if (multi) return Array.isArray(value) && value.includes(val);
    return String(value) === String(val);
  };

  const selectedOptions = options.filter(o => isSelected(o.value));
  
  const getTriggerText = () => {
    if (selectedOptions.length === 0) return placeholder;
    if (multi) {
      if (selectedOptions.length === 1) return selectedOptions[0].label;
      return `${selectedOptions.length} items selected`;
    }
    return selectedOptions[0].label;
  };

  const handleOptionClick = (opt) => {
    if (multi) {
      const currentValues = Array.isArray(value) ? value : [];
      let newValues;
      if (currentValues.includes(opt.value)) {
        newValues = currentValues.filter(v => v !== opt.value);
      } else {
        newValues = [...currentValues, opt.value];
      }
      onChange({ target: { value: newValues } });
    } else {
      onChange({ target: { value: opt.value } });
      setIsOpen(false);
    }
  };

  return (
    <div ref={containerRef} className={`fluent-select-container ${className}`} style={{ position: 'relative', width: '100%', ...style }}>
      <div 
        className={`fluent-select-trigger ${isOpen ? 'open' : ''} ${selectedOptions.length === 0 ? 'placeholder' : ''}`}
        onClick={() => setIsOpen(!isOpen)}
        role="button"
        aria-haspopup="listbox"
        aria-expanded={isOpen}
      >
        <span className="fluent-select-value">
          {getTriggerText()}
        </span>
        <motion.span 
          className="fluent-select-icon"
          animate={{ rotate: isOpen ? 180 : 0 }}
          transition={{ duration: 0.2 }}
        >
          <FiChevronDown />
        </motion.span>
      </div>
      
      <AnimatePresence>
        {isOpen && (
          <motion.div 
            initial={{ opacity: 0, y: -10, scale: 0.95 }}
            animate={{ opacity: 1, y: 6, scale: 1 }}
            exit={{ opacity: 0, scale: 0.95 }}
            transition={{ type: "spring", damping: 20, stiffness: 300 }}
            className="fluent-select-dropdown"
            style={{ zIndex: 1000 }}
          >
            {options.map((opt, idx) => {
              const selected = isSelected(opt.value);
              return (
                <motion.div 
                  key={opt.value} 
                  initial={{ opacity: 0, x: -5 }}
                  animate={{ opacity: 1, x: 0, transition: { delay: idx * 0.01 } }}
                  className={`fluent-select-option ${selected ? 'selected' : ''}`}
                  onClick={() => handleOptionClick(opt)}
                  role="option"
                  aria-selected={selected}
                >
                  {multi && (
                    <div className={`fluent-checkbox ${selected ? 'checked' : ''}`}>
                      {selected && <FiCheck size={12} />}
                    </div>
                  )}
                  <span className="option-label">{opt.label}</span>
                </motion.div>
              );
            })}
            {options.length === 0 && (
              <div className="fluent-select-no-options">No results</div>
            )}
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
