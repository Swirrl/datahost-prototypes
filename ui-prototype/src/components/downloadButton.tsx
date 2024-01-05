import React, { ReactNode, ButtonHTMLAttributes } from 'react';

interface DownloadButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  children: ReactNode;
}

const DownloadButton: React.FC<DownloadButtonProps> = ({ children, ...props }) => {
  return (
    <button
      className="bg-green-700 hover:bg-green-800 text-white 
       py-2 px-4 focus:outline-none focus:shadow-outline 
       ease-in-out"
      {...props}
    >
      {children}
    </button>
  );
};

export default DownloadButton;